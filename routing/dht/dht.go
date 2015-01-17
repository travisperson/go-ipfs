// Package dht implements a distributed hash table that satisfies the ipfs routing
// interface. This DHT is modeled after kademlia with Coral and S/Kademlia modifications.
package dht

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	ci "github.com/jbenet/go-ipfs/p2p/crypto"
	host "github.com/jbenet/go-ipfs/p2p/host"
	peer "github.com/jbenet/go-ipfs/p2p/peer"
	protocol "github.com/jbenet/go-ipfs/p2p/protocol"
	routing "github.com/jbenet/go-ipfs/routing"
	pb "github.com/jbenet/go-ipfs/routing/dht/pb"
	kb "github.com/jbenet/go-ipfs/routing/kbucket"
	u "github.com/jbenet/go-ipfs/util"
	"github.com/jbenet/go-ipfs/util/eventlog"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	"github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/goprotobuf/proto"
	ctxgroup "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-ctxgroup"
	ds "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore"
)

var log = eventlog.Logger("dht")

var ProtocolDHT protocol.ID = "/ipfs/dht"

const doPinging = false

// NumBootstrapQueries defines the number of random dht queries to do to
// collect members of the routing table.
const NumBootstrapQueries = 5

// TODO. SEE https://github.com/jbenet/node-ipfs/blob/master/submodules/ipfs-dht/index.js

// IpfsDHT is an implementation of Kademlia with Coral and S/Kademlia modifications.
// It is used to implement the base IpfsRouting module.
type IpfsDHT struct {
	host      host.Host      // the network services we need
	self      peer.ID        // Local peer (yourself)
	peerstore peer.Peerstore // Peer Registry

	datastore ds.ThreadSafeDatastore // Local data

	routingTable *kb.RoutingTable // Array of routing tables for differently distanced nodes
	providers    *ProviderManager

	birth    time.Time  // When this peer started up
	diaglock sync.Mutex // lock to make diagnostics work better

	// record validator funcs
	Validators map[string]ValidatorFunc

	ctxgroup.ContextGroup
}

// NewDHT creates a new DHT object with the given peer as the 'local' host
func NewDHT(ctx context.Context, h host.Host, dstore ds.ThreadSafeDatastore) *IpfsDHT {
	dht := new(IpfsDHT)
	dht.datastore = dstore
	dht.self = h.ID()
	dht.peerstore = h.Peerstore()
	dht.ContextGroup = ctxgroup.WithContext(ctx)
	dht.host = h

	// sanity check. this should **never** happen
	if len(dht.peerstore.Addresses(dht.self)) < 1 {
		panic("attempt to initialize dht without addresses for self")
	}

	h.SetStreamHandler(ProtocolDHT, dht.handleNewStream)
	dht.providers = NewProviderManager(dht.Context(), dht.self)
	dht.AddChildGroup(dht.providers)

	dht.routingTable = kb.NewRoutingTable(20, kb.ConvertPeerID(dht.self), time.Minute, dht.peerstore)
	dht.birth = time.Now()

	dht.Validators = make(map[string]ValidatorFunc)
	dht.Validators["pk"] = ValidatePublicKeyRecord

	if doPinging {
		dht.Children().Add(1)
		go dht.PingRoutine(time.Second * 10)
	}
	return dht
}

// LocalPeer returns the peer.Peer of the dht.
func (dht *IpfsDHT) LocalPeer() peer.ID {
	return dht.self
}

// log returns the dht's logger
func (dht *IpfsDHT) log() eventlog.EventLogger {
	return log.Prefix("dht(%s)", dht.self)
}

// Connect to a new peer at the given address, ping and add to the routing table
func (dht *IpfsDHT) Connect(ctx context.Context, npeer peer.ID) error {
	// TODO: change interface to accept a PeerInfo as well.
	if err := dht.host.Connect(ctx, peer.PeerInfo{ID: npeer}); err != nil {
		return err
	}

	// Ping new peer to register in their routing table
	// NOTE: this should be done better...
	if _, err := dht.Ping(ctx, npeer); err != nil {
		return fmt.Errorf("failed to ping newly connected peer: %s", err)
	}
	log.Event(ctx, "connect", dht.self, npeer)
	dht.Update(ctx, npeer)
	return nil
}

// putValueToPeer stores the given key/value pair at the peer 'p'
func (dht *IpfsDHT) putValueToPeer(ctx context.Context, p peer.ID,
	key u.Key, rec *pb.Record) error {

	pmes := pb.NewMessage(pb.Message_PUT_VALUE, string(key), 0)
	pmes.Record = rec
	rpmes, err := dht.sendRequest(ctx, p, pmes)
	if err != nil {
		return err
	}

	if !bytes.Equal(rpmes.GetRecord().Value, pmes.GetRecord().Value) {
		return errors.New("value not put correctly")
	}
	return nil
}

// putProvider sends a message to peer 'p' saying that the local node
// can provide the value of 'key'
func (dht *IpfsDHT) putProvider(ctx context.Context, p peer.ID, key string) error {

	// add self as the provider
	pi := dht.peerstore.PeerInfo(dht.self)
	// // only share WAN-friendly addresses ??
	// pi.Addrs = addrutil.WANShareableAddrs(pi.Addrs)
	if len(pi.Addrs) < 1 {
		log.Errorf("%s putProvider: %s for %s error: no wan-friendly addresses", dht.self, p, u.Key(key), pi.Addrs)
		return fmt.Errorf("no known addresses for self. cannot put provider.")
	}

	pmes := pb.NewMessage(pb.Message_ADD_PROVIDER, string(key), 0)
	pmes.ProviderPeers = pb.PeerInfosToPBPeers(dht.host.Network(), []peer.PeerInfo{pi})
	err := dht.sendMessage(ctx, p, pmes)
	if err != nil {
		return err
	}

	log.Debugf("%s putProvider: %s for %s (%s)", dht.self, p, u.Key(key), pi.Addrs)
	return nil
}

// getValueOrPeers queries a particular peer p for the value for
// key. It returns either the value or a list of closer peers.
// NOTE: it will update the dht's peerstore with any new addresses
// it finds for the given peer.
func (dht *IpfsDHT) getValueOrPeers(ctx context.Context, p peer.ID,
	key u.Key) ([]byte, []peer.PeerInfo, error) {

	pmes, err := dht.getValueSingle(ctx, p, key)
	if err != nil {
		return nil, nil, err
	}

	if record := pmes.GetRecord(); record != nil {
		// Success! We were given the value
		log.Debug("getValueOrPeers: got value")

		// make sure record is valid.
		err = dht.verifyRecordOnline(ctx, record)
		if err != nil {
			log.Error("Received invalid record!")
			return nil, nil, err
		}
		return record.GetValue(), nil, nil
	}

	// Perhaps we were given closer peers
	peers := pb.PBPeersToPeerInfos(pmes.GetCloserPeers())
	if len(peers) > 0 {
		log.Debug("getValueOrPeers: peers")
		return nil, peers, nil
	}

	log.Warning("getValueOrPeers: routing.ErrNotFound")
	return nil, nil, routing.ErrNotFound
}

// getValueSingle simply performs the get value RPC with the given parameters
func (dht *IpfsDHT) getValueSingle(ctx context.Context, p peer.ID,
	key u.Key) (*pb.Message, error) {
	defer log.EventBegin(ctx, "getValueSingle", p, &key).Done()

	pmes := pb.NewMessage(pb.Message_GET_VALUE, string(key), 0)
	return dht.sendRequest(ctx, p, pmes)
}

// getLocal attempts to retrieve the value from the datastore
func (dht *IpfsDHT) getLocal(key u.Key) ([]byte, error) {

	log.Debug("getLocal %s", key)
	v, err := dht.datastore.Get(key.DsKey())
	if err != nil {
		return nil, err
	}
	log.Debug("found in db")

	byt, ok := v.([]byte)
	if !ok {
		return nil, errors.New("value stored in datastore not []byte")
	}
	rec := new(pb.Record)
	err = proto.Unmarshal(byt, rec)
	if err != nil {
		return nil, err
	}

	// TODO: 'if paranoid'
	if u.Debug {
		err = dht.verifyRecordLocally(rec)
		if err != nil {
			log.Errorf("local record verify failed: %s", err)
			return nil, err
		}
	}

	return rec.GetValue(), nil
}

// getOwnPrivateKey attempts to load the local peers private
// key from the peerstore.
func (dht *IpfsDHT) getOwnPrivateKey() (ci.PrivKey, error) {
	sk := dht.peerstore.PrivKey(dht.self)
	if sk == nil {
		log.Errorf("%s dht cannot get own private key!", dht.self)
		return nil, fmt.Errorf("cannot get private key to sign record!")
	}
	return sk, nil
}

// putLocal stores the key value pair in the datastore
func (dht *IpfsDHT) putLocal(key u.Key, value []byte) error {
	sk, err := dht.getOwnPrivateKey()
	if err != nil {
		return err
	}

	rec, err := MakePutRecord(sk, key, value)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(rec)
	if err != nil {
		return err
	}

	return dht.datastore.Put(key.DsKey(), data)
}

// Update signals the routingTable to Update its last-seen status
// on the given peer.
func (dht *IpfsDHT) Update(ctx context.Context, p peer.ID) {
	log.Event(ctx, "updatePeer", p)
	dht.routingTable.Update(p)
}

// FindLocal looks for a peer with a given ID connected to this dht and returns the peer and the table it was found in.
func (dht *IpfsDHT) FindLocal(id peer.ID) peer.PeerInfo {
	p := dht.routingTable.Find(id)
	if p != "" {
		return dht.peerstore.PeerInfo(p)
	}
	return peer.PeerInfo{}
}

// findPeerSingle asks peer 'p' if they know where the peer with id 'id' is
func (dht *IpfsDHT) findPeerSingle(ctx context.Context, p peer.ID, id peer.ID) (*pb.Message, error) {
	defer log.EventBegin(ctx, "findPeerSingle", p, id).Done()

	pmes := pb.NewMessage(pb.Message_FIND_NODE, string(id), 0)
	return dht.sendRequest(ctx, p, pmes)
}

func (dht *IpfsDHT) findProvidersSingle(ctx context.Context, p peer.ID, key u.Key) (*pb.Message, error) {
	defer log.EventBegin(ctx, "findProvidersSingle", p, &key).Done()

	pmes := pb.NewMessage(pb.Message_GET_PROVIDERS, string(key), 0)
	return dht.sendRequest(ctx, p, pmes)
}

// nearestPeersToQuery returns the routing tables closest peers.
func (dht *IpfsDHT) nearestPeersToQuery(pmes *pb.Message, count int) []peer.ID {
	key := u.Key(pmes.GetKey())
	closer := dht.routingTable.NearestPeers(kb.ConvertKey(key), count)
	return closer
}

// betterPeerToQuery returns nearestPeersToQuery, but iff closer than self.
func (dht *IpfsDHT) betterPeersToQuery(pmes *pb.Message, p peer.ID, count int) []peer.ID {
	closer := dht.nearestPeersToQuery(pmes, count)

	// no node? nil
	if closer == nil {
		return nil
	}

	// == to self? thats bad
	for _, p := range closer {
		if p == dht.self {
			log.Error("Attempted to return self! this shouldnt happen...")
			return nil
		}
	}

	var filtered []peer.ID
	for _, clp := range closer {
		// Dont send a peer back themselves
		if p == clp {
			continue
		}

		// must all be closer than self
		key := u.Key(pmes.GetKey())
		if !kb.Closer(dht.self, clp, key) {
			filtered = append(filtered, clp)
		}
	}

	// ok seems like closer nodes
	return filtered
}

func (dht *IpfsDHT) ensureConnectedToPeer(ctx context.Context, p peer.ID) error {
	if p == dht.self {
		return errors.New("attempting to ensure connection to self")
	}

	// dial connection
	return dht.host.Connect(ctx, peer.PeerInfo{ID: p})
}

// PingRoutine periodically pings nearest neighbors.
func (dht *IpfsDHT) PingRoutine(t time.Duration) {
	defer dht.Children().Done()

	tick := time.Tick(t)
	for {
		select {
		case <-tick:
			id := make([]byte, 16)
			rand.Read(id)
			peers := dht.routingTable.NearestPeers(kb.ConvertKey(u.Key(id)), 5)
			for _, p := range peers {
				ctx, _ := context.WithTimeout(dht.Context(), time.Second*5)
				_, err := dht.Ping(ctx, p)
				if err != nil {
					log.Errorf("Ping error: %s", err)
				}
			}
		case <-dht.Closing():
			return
		}
	}
}

// Bootstrap builds up list of peers by requesting random peer IDs
func (dht *IpfsDHT) Bootstrap(ctx context.Context, queries int) error {
	var merr u.MultiErr

	randomID := func() peer.ID {
		// 16 random bytes is not a valid peer id. it may be fine becuase
		// the dht will rehash to its own keyspace anyway.
		id := make([]byte, 16)
		rand.Read(id)
		return peer.ID(id)
	}

	// bootstrap sequentially, as results will compound
	runQuery := func(ctx context.Context, id peer.ID) {
		p, err := dht.FindPeer(ctx, id)
		if err == routing.ErrNotFound {
			// this isn't an error. this is precisely what we expect.
		} else if err != nil {
			merr = append(merr, err)
		} else {
			// woah, actually found a peer with that ID? this shouldn't happen normally
			// (as the ID we use is not a real ID). this is an odd error worth logging.
			err := fmt.Errorf("Bootstrap peer error: Actually FOUND peer. (%s, %s)", id, p)
			log.Errorf("%s", err)
			merr = append(merr, err)
		}
	}

	sequential := true
	if sequential {
		// these should be parallel normally. but can make them sequential for debugging.
		// note that the core/bootstrap context deadline should be extended too for that.
		for i := 0; i < queries; i++ {
			id := randomID()
			log.Debugf("Bootstrapping query (%d/%d) to random ID: %s", i+1, queries, id)
			runQuery(ctx, id)
		}

	} else {
		// note on parallelism here: the context is passed in to the queries, so they
		// **should** exit when it exceeds, making this function exit on ctx cancel.
		// normally, we should be selecting on ctx.Done() here too, but this gets
		// complicated to do with WaitGroup, and doesnt wait for the children to exit.
		var wg sync.WaitGroup
		for i := 0; i < queries; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				id := randomID()
				log.Debugf("Bootstrapping query (%d/%d) to random ID: %s", i+1, queries, id)
				runQuery(ctx, id)
			}()
		}
		wg.Wait()
	}

	if len(merr) > 0 {
		return merr
	}
	return nil
}
