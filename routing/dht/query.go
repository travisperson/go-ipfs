package dht

import (
	"sync"

	peer "github.com/jbenet/go-ipfs/p2p/peer"
	queue "github.com/jbenet/go-ipfs/p2p/peer/queue"
	"github.com/jbenet/go-ipfs/routing"
	u "github.com/jbenet/go-ipfs/util"
	eventlog "github.com/jbenet/go-ipfs/util/eventlog"
	pset "github.com/jbenet/go-ipfs/util/peerset"
	todoctr "github.com/jbenet/go-ipfs/util/todocounter"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	ctxgroup "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-ctxgroup"
)

var maxQueryConcurrency = AlphaValue

type dhtQuery struct {
	dht         *IpfsDHT
	key         u.Key     // the key we're querying for
	qfunc       queryFunc // the function to execute per peer
	concurrency int       // the concurrency parameter
}

type dhtQueryResult struct {
	value         []byte          // GetValue
	peer          peer.PeerInfo   // FindPeer
	providerPeers []peer.PeerInfo // GetProviders
	closerPeers   []peer.PeerInfo // *
	success       bool
}

// constructs query
func (dht *IpfsDHT) newQuery(k u.Key, f queryFunc) *dhtQuery {
	return &dhtQuery{
		key:         k,
		dht:         dht,
		qfunc:       f,
		concurrency: maxQueryConcurrency,
	}
}

// QueryFunc is a function that runs a particular query with a given peer.
// It returns either:
// - the value
// - a list of peers potentially better able to serve the query
// - an error
type queryFunc func(context.Context, peer.ID) (*dhtQueryResult, error)

// Run runs the query at hand. pass in a list of peers to use first.
func (q *dhtQuery) Run(ctx context.Context, peers []peer.ID) (*dhtQueryResult, error) {
	runner := newQueryRunner(ctx, q)
	return runner.Run(peers)
}

type dhtQueryRunner struct {
	query          *dhtQuery        // query to run
	peersSeen      *pset.PeerSet    // all peers queried. prevent querying same peer 2x
	peersToQuery   *queue.ChanQueue // peers remaining to be queried
	peersRemaining todoctr.Counter  // peersToQuery + currently processing

	result *dhtQueryResult // query result
	errs   []error         // result errors. maybe should be a map[peer.ID]error

	rateLimit chan struct{} // processing semaphore
	log       eventlog.EventLogger

	cg ctxgroup.ContextGroup
	sync.RWMutex
}

func newQueryRunner(ctx context.Context, q *dhtQuery) *dhtQueryRunner {
	return &dhtQueryRunner{
		query:          q,
		peersToQuery:   queue.NewChanQueue(ctx, queue.NewXORDistancePQ(q.key)),
		peersRemaining: todoctr.NewSyncCounter(),
		peersSeen:      pset.New(),
		rateLimit:      make(chan struct{}, q.concurrency),
		cg:             ctxgroup.WithContext(ctx),
	}
}

func (r *dhtQueryRunner) Run(peers []peer.ID) (*dhtQueryResult, error) {
	log := log.Prefix("dht(%s).Query(%s).Run(%d)", r.query.dht.self, r.query.key, len(peers))
	r.log = log
	log.Debug("enter")
	defer log.Debug("end")

	log.Debugf("Run query with %d peers.", len(peers))
	if len(peers) == 0 {
		log.Warning("Running query with no peers!")
		return nil, nil
	}

	// setup concurrency rate limiting
	for i := 0; i < r.query.concurrency; i++ {
		r.rateLimit <- struct{}{}
	}

	// add all the peers we got first.
	for _, p := range peers {
		r.addPeerToQuery(r.cg.Context(), p)
	}

	// go do this thing.
	// do it as a child func to make sure Run exits
	// ONLY AFTER spawn workers has exited.
	log.Debugf("go spawn workers")
	r.cg.AddChildFunc(r.spawnWorkers)

	// so workers are working.

	// wait until they're done.
	err := routing.ErrNotFound

	select {
	case <-r.peersRemaining.Done():
		log.Debug("all peers ended")
		r.cg.Close()
		r.RLock()
		defer r.RUnlock()

		if len(r.errs) > 0 {
			err = r.errs[0] // take the first?
		}

	case <-r.cg.Closed():
		log.Debug("r.cg.Closed()")

		r.RLock()
		defer r.RUnlock()
		err = r.cg.Context().Err() // collect the error.
	}

	if r.result != nil && r.result.success {
		log.Debug("success: %s", r.result)
		return r.result, nil
	}

	log.Debug("failure: %s", err)
	return nil, err
}

func (r *dhtQueryRunner) addPeerToQuery(ctx context.Context, next peer.ID) {
	// if new peer is ourselves...
	if next == r.query.dht.self {
		r.log.Debug("addPeerToQuery skip self")
		return
	}

	if !r.peersSeen.TryAdd(next) {
		r.log.Debugf("addPeerToQuery skip seen %s", next)
		return
	}

	r.log.Debugf("addPeerToQuery adding %s", next)
	r.peersRemaining.Increment(1)
	select {
	case r.peersToQuery.EnqChan <- next:
	case <-ctx.Done():
	}
}

func (r *dhtQueryRunner) spawnWorkers(parent ctxgroup.ContextGroup) {
	log := r.log.Prefix("spawnWorkers")
	log.Debugf("begin")
	defer log.Debugf("end")

	for {

		select {
		case <-r.peersRemaining.Done():
			return

		case <-r.cg.Closing():
			return

		case p, more := <-r.peersToQuery.DeqChan:
			if !more {
				return // channel closed.
			}
			log.Debugf("spawning worker for: %v", p)

			// do it as a child func to make sure Run exits
			// ONLY AFTER spawn workers has exited.
			parent.AddChildFunc(func(cg ctxgroup.ContextGroup) {
				r.queryPeer(cg, p)
			})
		}
	}
}

func (r *dhtQueryRunner) queryPeer(cg ctxgroup.ContextGroup, p peer.ID) {
	log := r.log.Prefix("queryPeer(%s)", p)
	log.Debugf("spawned")
	defer log.Debugf("finished")

	// make sure we rate limit concurrency.
	select {
	case <-r.rateLimit:
	case <-cg.Closing():
		r.peersRemaining.Decrement(1)
		return
	}

	// ok let's do this!
	log.Debugf("running")

	// make sure we do this when we exit
	defer func() {
		// signal we're done proccessing peer p
		log.Debugf("completed")
		r.peersRemaining.Decrement(1)
		r.rateLimit <- struct{}{}
	}()

	// make sure we're connected to the peer.
	if conns := r.query.dht.host.Network().ConnsToPeer(p); len(conns) == 0 {
		log.Infof("not connected. dialing.")

		pi := peer.PeerInfo{ID: p}
		if err := r.query.dht.host.Connect(cg.Context(), pi); err != nil {
			log.Debugf("Error connecting: %s", err)
			r.Lock()
			r.errs = append(r.errs, err)
			r.Unlock()
			return
		}

		log.Debugf("connected. dial success.")
	}

	// finally, run the query against this peer
	log.Debugf("query running")
	res, err := r.query.qfunc(cg.Context(), p)
	log.Debugf("query finished")

	if err != nil {
		log.Debugf("ERROR worker for: %v %v", p, err)
		r.Lock()
		r.errs = append(r.errs, err)
		r.Unlock()

	} else if res.success {
		log.Debugf("SUCCESS worker for: %v %s", p, res)
		r.Lock()
		r.result = res
		r.Unlock()
		go r.cg.Close() // signal to everyone that we're done.
		// must be async, as we're one of the children, and Close blocks.

	} else if len(res.closerPeers) > 0 {
		log.Debugf("PEERS CLOSER -- worker for: %v (%d closer peers)", p, len(res.closerPeers))
		for _, next := range res.closerPeers {
			if next.ID == r.query.dht.self { // dont add self.
				log.Debugf("PEERS CLOSER -- worker for: %v found self", p)
				continue
			}

			// add their addresses to the dialer's peerstore
			r.query.dht.peerstore.AddPeerInfo(next)
			r.addPeerToQuery(cg.Context(), next.ID)
			log.Debugf("PEERS CLOSER -- worker for: %v added %v (%v)", p, next.ID, next.Addrs)
		}
	} else {
		log.Debugf("QUERY worker for: %v - not found, and no closer peers.", p)
	}
}
