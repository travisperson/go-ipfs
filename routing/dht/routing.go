package dht

import (
	"sync"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"

	inet "github.com/jbenet/go-ipfs/net"
	peer "github.com/jbenet/go-ipfs/peer"
	"github.com/jbenet/go-ipfs/routing"
	pb "github.com/jbenet/go-ipfs/routing/dht/pb"
	kb "github.com/jbenet/go-ipfs/routing/kbucket"
	u "github.com/jbenet/go-ipfs/util"
)

// asyncQueryBuffer is the size of buffered channels in async queries. This
// buffer allows multiple queries to execute simultaneously, return their
// results and continue querying closer peers. Note that different query
// results will wait for the channel to drain.
var asyncQueryBuffer = 10

// This file implements the Routing interface for the IpfsDHT struct.

// Basic Put/Get

// PutValue adds value corresponding to given Key.
// This is the top level "Store" operation of the DHT
func (dht *IpfsDHT) PutValue(ctx context.Context, key u.Key, value []byte) error {
	log.Debugf("PutValue %s", key)
	err := dht.putLocal(key, value)
	if err != nil {
		return err
	}

	rec, err := dht.makePutRecord(key, value)
	if err != nil {
		log.Error("Creation of record failed!")
		return err
	}

	peers := dht.routingTable.NearestPeers(kb.ConvertKey(key), KValue)

	query := newQuery(key, dht.dialer, func(ctx context.Context, p peer.Peer) (*dhtQueryResult, error) {
		log.Debugf("%s PutValue qry part %v", dht.self, p)
		err := dht.putValueToNetwork(ctx, p, string(key), rec)
		if err != nil {
			return nil, err
		}
		return &dhtQueryResult{success: true}, nil
	})

	_, err = query.Run(ctx, peers)
	return err
}

// GetValue searches for the value corresponding to given Key.
// If the search does not succeed, a multiaddr string of a closer peer is
// returned along with util.ErrSearchIncomplete
func (dht *IpfsDHT) GetValue(ctx context.Context, key u.Key) ([]byte, error) {
	log.Debugf("Get Value [%s]", key)

	// If we have it local, dont bother doing an RPC!
	// NOTE: this might not be what we want to do...
	val, err := dht.getLocal(key)
	if err == nil {
		log.Debug("Got value locally!")
		return val, nil
	}

	// get closest peers in the routing table
	closest := dht.routingTable.NearestPeers(kb.ConvertKey(key), PoolSize)
	if closest == nil || len(closest) == 0 {
		log.Warning("Got no peers back from routing table!")
		return nil, kb.ErrLookupFailure
	}

	// setup the Query
	query := newQuery(key, dht.dialer, func(ctx context.Context, p peer.Peer) (*dhtQueryResult, error) {

		val, peers, err := dht.getValueOrPeers(ctx, p, key)
		if err != nil {
			return nil, err
		}

		res := &dhtQueryResult{value: val, closerPeers: peers}
		if val != nil {
			res.success = true
		}

		return res, nil
	})

	// run it!
	result, err := query.Run(ctx, closest)
	if err != nil {
		return nil, err
	}

	log.Debugf("GetValue %v %v", key, result.value)
	if result.value == nil {
		return nil, routing.ErrNotFound
	}

	return result.value, nil
}

// Value provider layer of indirection.
// This is what DSHTs (Coral and MainlineDHT) do to store large values in a DHT.

// Provide makes this node announce that it can provide a value for the given key
func (dht *IpfsDHT) Provide(ctx context.Context, key u.Key) error {

	dht.providers.AddProvider(key, dht.self)
	peers := dht.routingTable.NearestPeers(kb.ConvertKey(key), PoolSize)
	if len(peers) == 0 {
		return nil
	}

	//TODO FIX: this doesn't work! it needs to be sent to the actual nearest peers.
	// `peers` are the closest peers we have, not the ones that should get the value.
	for _, p := range peers {
		err := dht.putProvider(ctx, p, string(key))
		if err != nil {
			return err
		}
	}
	return nil
}

// FindProvidersAsync is the same thing as FindProviders, but returns a channel.
// Peers will be returned on the channel as soon as they are found, even before
// the search query completes.
func (dht *IpfsDHT) FindProvidersAsync(ctx context.Context, key u.Key, count int) <-chan peer.Peer {
	log.Event(ctx, "findProviders", &key)
	peerOut := make(chan peer.Peer, count)
	go dht.findProvidersAsyncRoutine(ctx, key, count, peerOut)
	return peerOut
}

func (dht *IpfsDHT) findProvidersAsyncRoutine(ctx context.Context, key u.Key, count int, peerOut chan peer.Peer) {
	defer close(peerOut)

	ps := newPeerSet()
	provs := dht.providers.GetProviders(ctx, key)
	for _, p := range provs {
		// NOTE: assuming that this list of peers is unique
		if ps.AddIfSmallerThan(p, count) {
			select {
			case peerOut <- p:
			case <-ctx.Done():
				return
			}
		}

		// If we have enough peers locally, dont bother with remote RPC
		if ps.Size() >= count {
			return
		}
	}

	// setup the Query
	query := newQuery(key, dht.dialer, func(ctx context.Context, p peer.Peer) (*dhtQueryResult, error) {

		pmes, err := dht.findProvidersSingle(ctx, p, key)
		if err != nil && err != routing.ErrNotFound {
			return nil, err
		}

		provs, errs := pb.PBPeersToPeers(dht.peerstore, pmes.GetProviderPeers())
		for _, err := range errs {
			if err != nil {
				log.Warning(err)
			}
		}

		// Add unique providers from request, up to 'count'
		for _, prov := range provs {
			if ps.AddIfSmallerThan(prov, count) {
				select {
				case peerOut <- prov:
				case <-ctx.Done():
					log.Error("Context timed out sending more providers")
					return nil, ctx.Err()
				}
			}
			if ps.Size() >= count {
				return &dhtQueryResult{success: true}, nil
			}
		}

		// Give closer peers back to the query to be queried
		closer := pmes.GetCloserPeers()
		clpeers, errs := pb.PBPeersToPeers(dht.peerstore, closer)
		for _, err := range errs {
			if err != nil {
				log.Warning(err)
			}
		}

		return &dhtQueryResult{closerPeers: clpeers}, nil
	})

	peers := dht.routingTable.NearestPeers(kb.ConvertKey(key), AlphaValue)
	_, err := query.Run(ctx, peers)
	if err != nil {
		log.Errorf("FindProviders Query error: %s", err)
	}
}

func (dht *IpfsDHT) addPeerListAsync(ctx context.Context, k u.Key, peers []*pb.Message_Peer, ps *peerSet, count int, out chan peer.Peer) {
	var wg sync.WaitGroup
	for _, pbp := range peers {
		wg.Add(1)
		go func(mp *pb.Message_Peer) {
			defer wg.Done()
			// construct new peer
			p, err := dht.ensureConnectedToPeer(ctx, mp)
			if err != nil {
				log.Errorf("%s", err)
				return
			}
			if p == nil {
				log.Error("Got nil peer from ensureConnectedToPeer")
				return
			}

			dht.providers.AddProvider(k, p)
			if ps.AddIfSmallerThan(p, count) {
				select {
				case out <- p:
				case <-ctx.Done():
					return
				}
			} else if ps.Size() >= count {
				return
			}
		}(pbp)
	}
	wg.Wait()
}

// FindPeer searches for a peer with given ID.
func (dht *IpfsDHT) FindPeer(ctx context.Context, id peer.ID) (peer.Peer, error) {

	// Check if were already connected to them
	p, _ := dht.FindLocal(id)
	if p != nil {
		return p, nil
	}

	closest := dht.routingTable.NearestPeers(kb.ConvertPeerID(id), AlphaValue)
	if closest == nil || len(closest) == 0 {
		return nil, kb.ErrLookupFailure
	}

	// Sanity...
	for _, p := range closest {
		if p.ID().Equal(id) {
			log.Error("Found target peer in list of closest peers...")
			return p, nil
		}
	}

	// setup the Query
	query := newQuery(u.Key(id), dht.dialer, func(ctx context.Context, p peer.Peer) (*dhtQueryResult, error) {

		pmes, err := dht.findPeerSingle(ctx, p, id)
		if err != nil {
			return nil, err
		}

		closer := pmes.GetCloserPeers()
		clpeers, errs := pb.PBPeersToPeers(dht.peerstore, closer)
		for _, err := range errs {
			if err != nil {
				log.Warning(err)
			}
		}

		// see it we got the peer here
		for _, np := range clpeers {
			if string(np.ID()) == string(id) {
				return &dhtQueryResult{
					peer:    np,
					success: true,
				}, nil
			}
		}

		return &dhtQueryResult{closerPeers: clpeers}, nil
	})

	// run it!
	result, err := query.Run(ctx, closest)
	if err != nil {
		return nil, err
	}

	log.Debugf("FindPeer %v %v", id, result.success)
	if result.peer == nil {
		return nil, routing.ErrNotFound
	}

	return result.peer, nil
}

// FindPeersConnectedToPeer searches for peers directly connected to a given peer.
func (dht *IpfsDHT) FindPeersConnectedToPeer(ctx context.Context, id peer.ID) (<-chan peer.Peer, error) {

	peerchan := make(chan peer.Peer, asyncQueryBuffer)
	peersSeen := map[string]peer.Peer{}

	closest := dht.routingTable.NearestPeers(kb.ConvertPeerID(id), AlphaValue)
	if closest == nil || len(closest) == 0 {
		return nil, kb.ErrLookupFailure
	}

	// setup the Query
	query := newQuery(u.Key(id), dht.dialer, func(ctx context.Context, p peer.Peer) (*dhtQueryResult, error) {

		pmes, err := dht.findPeerSingle(ctx, p, id)
		if err != nil {
			return nil, err
		}

		var clpeers []peer.Peer
		closer := pmes.GetCloserPeers()
		for _, pbp := range closer {
			// skip peers already seen
			if _, found := peersSeen[string(pbp.GetId())]; found {
				continue
			}

			// skip peers that fail to unmarshal
			p, err := pb.PBPeerToPeer(dht.peerstore, pbp)
			if err != nil {
				log.Warning(err)
				continue
			}

			// if peer is connected, send it to our client.
			if pb.Connectedness(*pbp.Connection) == inet.Connected {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case peerchan <- p:
				}
			}

			peersSeen[string(p.ID())] = p

			// if peer is the peer we're looking for, don't bother querying it.
			if pb.Connectedness(*pbp.Connection) != inet.Connected {
				clpeers = append(clpeers, p)
			}
		}

		return &dhtQueryResult{closerPeers: clpeers}, nil
	})

	// run it! run it asynchronously to gen peers as results are found.
	// this does no error checking
	go func() {
		if _, err := query.Run(ctx, closest); err != nil {
			log.Error(err)
		}

		// close the peerchan channel when done.
		close(peerchan)
	}()

	return peerchan, nil
}

// Ping a peer, log the time it took
func (dht *IpfsDHT) Ping(ctx context.Context, p peer.Peer) error {
	// Thoughts: maybe this should accept an ID and do a peer lookup?
	log.Debugf("ping %s start", p)

	pmes := pb.NewMessage(pb.Message_PING, "", 0)
	_, err := dht.sendRequest(ctx, p, pmes)
	log.Debugf("ping %s end (err = %s)", p, err)
	return err
}
