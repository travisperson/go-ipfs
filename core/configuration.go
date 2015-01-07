package core

// important to keep as an interface to allow implementations to vary

import (
	"io"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	datastore "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore"
	sync "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore/sync"
	blockstore "github.com/jbenet/go-ipfs/blocks/blockstore"
	core_testutil "github.com/jbenet/go-ipfs/core/testutil"
	diag "github.com/jbenet/go-ipfs/diagnostics"
	exchange "github.com/jbenet/go-ipfs/exchange"
	bitswap "github.com/jbenet/go-ipfs/exchange/bitswap"
	bsnet "github.com/jbenet/go-ipfs/exchange/bitswap/network"
	importer "github.com/jbenet/go-ipfs/importer"
	chunk "github.com/jbenet/go-ipfs/importer/chunk"
	namesys "github.com/jbenet/go-ipfs/namesys"
	host "github.com/jbenet/go-ipfs/p2p/host"
	peer "github.com/jbenet/go-ipfs/p2p/peer"
	path "github.com/jbenet/go-ipfs/path"
	dht "github.com/jbenet/go-ipfs/routing/dht"
	uio "github.com/jbenet/go-ipfs/unixfs/io"
	util "github.com/jbenet/go-ipfs/util"
	"github.com/jbenet/go-ipfs/util/datastore2"
	ds2 "github.com/jbenet/go-ipfs/util/datastore2"
	delay "github.com/jbenet/go-ipfs/util/delay"
)

func (c *IpfsNode) ID() peer.ID {
	return c.repo.ID()
}

func (c *IpfsNode) Bootstrap(ctx context.Context, p peer.ID) error {
	return c.repo.Bootstrap(ctx, p)
}

func (c *IpfsNode) Cat(k util.Key) (io.Reader, error) {
	catterdag := c.DAG
	nodeCatted, err := (&path.Resolver{catterdag}).ResolvePath(k.String())
	if err != nil {
		return nil, err
	}
	return uio.NewDagReader(nodeCatted, catterdag)
}

func (c *IpfsNode) Add(r io.Reader) (util.Key, error) {
	nodeAdded, err := importer.BuildDagFromReader(
		r,
		c.DAG,
		nil,
		chunk.DefaultSplitter,
	)
	if err != nil {
		return "", err
	}
	return nodeAdded.Key()
}

type Config func(ctx context.Context) (Configuration, error)

type configuration struct {
	// DHT, Exchange, Network,Datastore
	bitSwapNetwork bsnet.BitSwapNetwork
	blockstore     blockstore.Blockstore
	exchange       exchange.Interface
	datastore      ds2.ThreadSafeDatastoreCloser
	host           host.Host
	dht            *dht.IpfsDHT
	id             peer.ID

	online           bool
	peerstore        peer.Peerstore
	diagnoticService *diag.Diagnostics
	nameSystem       namesys.NameSystem
}

func (c *configuration) Bootstrap(ctx context.Context, p peer.ID) error { return c.dht.Connect(ctx, p) }
func (d *configuration) OnlineMode() bool                               { return d.online }
func (d *configuration) Peerstore() peer.Peerstore                      { return d.peerstore }
func (r *configuration) Blockstore() blockstore.Blockstore              { return r.blockstore }
func (r *configuration) Datastore() ds2.ThreadSafeDatastoreCloser       { return r.datastore }
func (r *configuration) Exchange() exchange.Interface                   { return r.exchange }
func (r *configuration) ID() peer.ID                                    { return r.id }

func MocknetTestRepo(p peer.ID, h host.Host, conf core_testutil.LatencyConfig) Config {
	return func(ctx context.Context) (Configuration, error) {
		const kWriteCacheElems = 100
		const alwaysSendToPeer = true
		dsDelay := delay.Fixed(conf.BlockstoreLatency)
		ds := ds2.CloserWrap(sync.MutexWrap(datastore2.WithDelay(datastore.NewMapDatastore(), dsDelay)))

		log.Debugf("MocknetTestRepo: %s %s %s", p, h.ID(), h)
		dhtt := dht.NewDHT(ctx, h, ds)
		bsn := bsnet.NewFromIpfsHost(h, dhtt)
		bstore, err := blockstore.WriteCached(blockstore.NewBlockstore(ds), kWriteCacheElems)
		if err != nil {
			return nil, err
		}
		exch := bitswap.New(ctx, p, bsn, bstore, alwaysSendToPeer)
		return &configuration{
			bitSwapNetwork: bsn,
			blockstore:     bstore,
			exchange:       exch,
			datastore:      ds,
			host:           h,
			dht:            dhtt,
			id:             p,
		}, nil
	}
}
