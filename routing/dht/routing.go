package dht

import (
	"math"
	"sync"
	"time"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"

	inet "github.com/jbenet/go-ipfs/p2p/net"
	peer "github.com/jbenet/go-ipfs/p2p/peer"
	"github.com/jbenet/go-ipfs/routing"
	pb "github.com/jbenet/go-ipfs/routing/dht/pb"
	kb "github.com/jbenet/go-ipfs/routing/kbucket"
	record "github.com/jbenet/go-ipfs/routing/record"
	u "github.com/jbenet/go-ipfs/util"
	errors "github.com/jbenet/go-ipfs/util/debugerror"
	pset "github.com/jbenet/go-ipfs/util/peerset"
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

	sk, err := dht.getOwnPrivateKey()
	if err != nil {
		return err
	}

	rec, err := record.MakePutRecord(sk, key, value)
	if err != nil {
		log.Error("Creation of record failed!")
		return err
	}

	pchan, err := dht.getClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for p := range pchan {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			err := dht.putValueToPeer(ctx, p, key, rec)
			if err != nil {
				log.Errorf("failed putting value to peer: %s", err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}

// GetValue searches for the value corresponding to given Key.
// If the search does not succeed, a multiaddr string of a closer peer is
// returned along with util.ErrSearchIncomplete
func (dht *IpfsDHT) GetValue(ctx context.Context, key u.Key) ([]byte, error) {
	log := dht.log().Prefix("GetValue(%s)", key)
	log.Debugf("start")
	defer log.Debugf("end")

	// If we have it local, dont bother doing an RPC!
	val, err := dht.getLocal(key)
	if err == nil {
		log.Debug("have it locally")
		return val, nil
	} else {
		log.Debug("failed to get value locally: %s", err)
	}

	// get closest peers in the routing table
	rtp := dht.routingTable.ListPeers()
	log.Debugf("peers in rt: %s", len(rtp), rtp)

	closest := dht.routingTable.NearestPeers(kb.ConvertKey(key), PoolSize)
	if closest == nil || len(closest) == 0 {
		log.Warning("No peers from routing table!")
		return nil, errors.Wrap(kb.ErrLookupFailure)
	}

	// setup the Query
	query := dht.newQuery(key, func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
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
	log := dht.log().Prefix("Provide(%s)", key)

	defer log.EventBegin(ctx, "provide", &key).Done()

	// add self locally
	dht.providers.AddProvider(key, dht.self)

	peers, err := dht.getClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			log.Debugf("putProvider(%s, %s)", key, p)
			err := dht.putProvider(ctx, p, string(key))
			if err != nil {
				log.Error(err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}

// FindProviders searches until the context expires.
func (dht *IpfsDHT) FindProviders(ctx context.Context, key u.Key) ([]peer.PeerInfo, error) {
	var providers []peer.PeerInfo
	for p := range dht.FindProvidersAsync(ctx, key, math.MaxInt32) {
		providers = append(providers, p)
	}
	return providers, nil
}

// Kademlia 'node lookup' operation. Returns a channel of the K closest peers
// to the given key
func (dht *IpfsDHT) getClosestPeers(ctx context.Context, key u.Key) (<-chan peer.ID, error) {
	e := log.EventBegin(ctx, "getClosestPeers", &key)
	tablepeers := dht.routingTable.NearestPeers(kb.ConvertKey(key), AlphaValue)
	if len(tablepeers) == 0 {
		return nil, errors.Wrap(kb.ErrLookupFailure)
	}

	out := make(chan peer.ID, KValue)
	peerset := pset.NewLimited(KValue)

	for _, p := range tablepeers {
		select {
		case out <- p:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		peerset.Add(p)
	}

	query := dht.newQuery(key, func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		closer, err := dht.closerPeersSingle(ctx, key, p)
		if err != nil {
			log.Errorf("error getting closer peers: %s", err)
			return nil, err
		}

		var filtered []peer.PeerInfo
		for _, p := range closer {
			if kb.Closer(p, dht.self, key) && peerset.TryAdd(p) {
				select {
				case out <- p:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				filtered = append(filtered, dht.peerstore.PeerInfo(p))
			}
		}

		return &dhtQueryResult{closerPeers: filtered}, nil
	})

	go func() {
		defer close(out)
		defer e.Done()
		// run it!
		_, err := query.Run(ctx, tablepeers)
		if err != nil {
			log.Errorf("closestPeers query run error: %s", err)
		}
	}()

	return out, nil
}

func (dht *IpfsDHT) closerPeersSingle(ctx context.Context, key u.Key, p peer.ID) ([]peer.ID, error) {
	pmes, err := dht.findPeerSingle(ctx, p, peer.ID(key))
	if err != nil {
		return nil, err
	}

	var out []peer.ID
	for _, pbp := range pmes.GetCloserPeers() {
		pid := peer.ID(pbp.GetId())
		if pid != dht.self { // dont add self
			dht.peerstore.AddAddresses(pid, pbp.Addresses())
			out = append(out, pid)
		}
	}
	return out, nil
}

// FindProvidersAsync is the same thing as FindProviders, but returns a channel.
// Peers will be returned on the channel as soon as they are found, even before
// the search query completes.
func (dht *IpfsDHT) FindProvidersAsync(ctx context.Context, key u.Key, count int) <-chan peer.PeerInfo {
	log.Event(ctx, "findProviders", &key)
	peerOut := make(chan peer.PeerInfo, count)
	go dht.findProvidersAsyncRoutine(ctx, key, count, peerOut)
	return peerOut
}

func (dht *IpfsDHT) findProvidersAsyncRoutine(ctx context.Context, key u.Key, count int, peerOut chan peer.PeerInfo) {
	log := dht.log().Prefix("FindProviders(%s)", key)

	defer log.EventBegin(ctx, "findProvidersAsync", &key).Done()
	defer close(peerOut)

	ps := pset.NewLimited(count)
	provs := dht.providers.GetProviders(ctx, key)
	for _, p := range provs {
		// NOTE: assuming that this list of peers is unique
		if ps.TryAdd(p) {
			select {
			case peerOut <- dht.peerstore.PeerInfo(p):
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
	query := dht.newQuery(key, func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		log := log.Prefix("Query(%s)", p)
		log.Debugf("begin")
		defer log.Debugf("end")

		pmes, err := dht.findProvidersSingle(ctx, p, key)
		if err != nil {
			return nil, err
		}

		log.Debugf("%d provider entries", len(pmes.GetProviderPeers()))
		provs := pb.PBPeersToPeerInfos(pmes.GetProviderPeers())
		log.Debugf("%d provider entries decoded", len(provs))

		// Add unique providers from request, up to 'count'
		for _, prov := range provs {
			log.Debugf("got provider: %s", prov)
			if ps.TryAdd(prov.ID) {
				log.Debugf("using provider: %s", prov)
				select {
				case peerOut <- prov:
				case <-ctx.Done():
					log.Error("Context timed out sending more providers")
					return nil, ctx.Err()
				}
			}
			if ps.Size() >= count {
				log.Debugf("got enough providers (%d/%d)", ps.Size(), count)
				return &dhtQueryResult{success: true}, nil
			}
		}

		// Give closer peers back to the query to be queried
		closer := pmes.GetCloserPeers()
		clpeers := pb.PBPeersToPeerInfos(closer)
		log.Debugf("got closer peers: %d %s", len(clpeers), clpeers)
		return &dhtQueryResult{closerPeers: clpeers}, nil
	})

	peers := dht.routingTable.NearestPeers(kb.ConvertKey(key), AlphaValue)
	_, err := query.Run(ctx, peers)
	if err != nil {
		log.Errorf("Query error: %s", err)
	}
}

// FindPeer searches for a peer with given ID.
func (dht *IpfsDHT) FindPeer(ctx context.Context, id peer.ID) (peer.PeerInfo, error) {
	defer log.EventBegin(ctx, "FindPeer", id).Done()

	// Check if were already connected to them
	if pi := dht.FindLocal(id); pi.ID != "" {
		return pi, nil
	}

	closest := dht.routingTable.NearestPeers(kb.ConvertPeerID(id), AlphaValue)
	if closest == nil || len(closest) == 0 {
		return peer.PeerInfo{}, errors.Wrap(kb.ErrLookupFailure)
	}

	// Sanity...
	for _, p := range closest {
		if p == id {
			log.Error("Found target peer in list of closest peers...")
			return dht.peerstore.PeerInfo(p), nil
		}
	}

	// setup the Query
	query := dht.newQuery(u.Key(id), func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {

		pmes, err := dht.findPeerSingle(ctx, p, id)
		if err != nil {
			return nil, err
		}

		closer := pmes.GetCloserPeers()
		clpeerInfos := pb.PBPeersToPeerInfos(closer)

		// see it we got the peer here
		for _, npi := range clpeerInfos {
			if npi.ID == id {
				return &dhtQueryResult{
					peer:    npi,
					success: true,
				}, nil
			}
		}

		return &dhtQueryResult{closerPeers: clpeerInfos}, nil
	})

	// run it!
	result, err := query.Run(ctx, closest)
	if err != nil {
		return peer.PeerInfo{}, err
	}

	log.Debugf("FindPeer %v %v", id, result.success)
	if result.peer.ID == "" {
		return peer.PeerInfo{}, routing.ErrNotFound
	}

	return result.peer, nil
}

// FindPeersConnectedToPeer searches for peers directly connected to a given peer.
func (dht *IpfsDHT) FindPeersConnectedToPeer(ctx context.Context, id peer.ID) (<-chan peer.PeerInfo, error) {

	peerchan := make(chan peer.PeerInfo, asyncQueryBuffer)
	peersSeen := peer.Set{}

	closest := dht.routingTable.NearestPeers(kb.ConvertPeerID(id), AlphaValue)
	if closest == nil || len(closest) == 0 {
		return nil, errors.Wrap(kb.ErrLookupFailure)
	}

	// setup the Query
	query := dht.newQuery(u.Key(id), func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {

		pmes, err := dht.findPeerSingle(ctx, p, id)
		if err != nil {
			return nil, err
		}

		var clpeers []peer.PeerInfo
		closer := pmes.GetCloserPeers()
		for _, pbp := range closer {
			pi := pb.PBPeerToPeerInfo(pbp)

			// skip peers already seen
			if _, found := peersSeen[pi.ID]; found {
				continue
			}
			peersSeen[pi.ID] = struct{}{}

			// if peer is connected, send it to our client.
			if pb.Connectedness(*pbp.Connection) == inet.Connected {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case peerchan <- pi:
				}
			}

			// if peer is the peer we're looking for, don't bother querying it.
			// TODO maybe query it?
			if pb.Connectedness(*pbp.Connection) != inet.Connected {
				clpeers = append(clpeers, pi)
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
func (dht *IpfsDHT) Ping(ctx context.Context, p peer.ID) (time.Duration, error) {
	// Thoughts: maybe this should accept an ID and do a peer lookup?
	log.Debugf("ping %s start", p)
	before := time.Now()

	pmes := pb.NewMessage(pb.Message_PING, "", 0)
	_, err := dht.sendRequest(ctx, p, pmes)
	log.Debugf("ping %s end (err = %s)", p, err)

	return time.Now().Sub(before), err
}
