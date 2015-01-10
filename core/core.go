package core

import (
	"io"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	ctxgroup "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-ctxgroup"
	bstore "github.com/jbenet/go-ipfs/blocks/blockstore"
	bserv "github.com/jbenet/go-ipfs/blockservice"
	config "github.com/jbenet/go-ipfs/config"
	diag "github.com/jbenet/go-ipfs/diagnostics"
	exchange "github.com/jbenet/go-ipfs/exchange"
	mount "github.com/jbenet/go-ipfs/fuse/mount"
	importer "github.com/jbenet/go-ipfs/importer"
	chunk "github.com/jbenet/go-ipfs/importer/chunk"
	merkledag "github.com/jbenet/go-ipfs/merkledag"
	namesys "github.com/jbenet/go-ipfs/namesys"
	ic "github.com/jbenet/go-ipfs/p2p/crypto"
	p2phost "github.com/jbenet/go-ipfs/p2p/host"
	peer "github.com/jbenet/go-ipfs/p2p/peer"
	path "github.com/jbenet/go-ipfs/path"
	pin "github.com/jbenet/go-ipfs/pin"
	routing "github.com/jbenet/go-ipfs/routing"
	uio "github.com/jbenet/go-ipfs/unixfs/io"
	util "github.com/jbenet/go-ipfs/util"
	ds2 "github.com/jbenet/go-ipfs/util/datastore2"
	eventlog "github.com/jbenet/go-ipfs/util/eventlog"
)

const IpnsValidatorTag = "ipns"
const kSizeBlockstoreWriteCache = 100

var log = eventlog.Logger("core")

// IpfsNode is IPFS Core module. It represents an IPFS instance.
type IpfsNode struct {
	// Self
	PrivateKey ic.PrivKey // the local node's private Key
	onlineMode bool       // alternatively, offline

	// Local node
	Datastore ds2.ThreadSafeDatastoreCloser // the local datastore
	Pinning   pin.Pinner                    // the pinning manager
	Mounts    Mounts                        `allow:"nil"` // current mount state, if any.

	// Services
	Peerstore    peer.Peerstore       // storage for other Peer instances
	PeerHost     p2phost.Host         // the network host (server+client)
	Routing      routing.IpfsRouting  // the routing system. recommend ipfs-dht
	Exchange     exchange.Interface   // the block exchange + strategy (bitswap)
	Blockstore   bstore.Blockstore    // the block store (lower level)
	Blockservice *bserv.BlockService  // the block service, get/add blocks.
	DAG          merkledag.DAGService // the merkle dag service, get/add objects.
	Resolver     *path.Resolver       // the path resolution system
	Namesys      namesys.NameSystem   // the name system, resolves paths to hashes
	Diagnostics  *diag.Diagnostics    // the diagnostics service

	ctxgroup.ContextGroup

	id peer.ID
}

// Mounts defines what the node's mount state is. This should
// perhaps be moved to the daemon or mount. It's here because
// it needs to be accessible across daemon requests.
type Mounts struct {
	Ipfs mount.Mount
	Ipns mount.Mount
}

// NewIpfsNode constructs a new IpfsNode based on the given config.
// Deprecated.
func NewIpfsNode(ctx context.Context, cfg *config.Config, online bool) (*IpfsNode, error) {
	if online {
		return New(ctx, Online(cfg))
	}
	return New(ctx, Offline(cfg))
}

func New(parent context.Context, configure Config) (*IpfsNode, error) {
	ctxg := ctxgroup.WithContext(parent)
	ctx := ctxg.Context()
	success := false // flip to true after all sub-system inits succeed
	defer func() {
		if !success {
			ctxg.Close()
			// TODO handle close
		}
	}()
	n := &IpfsNode{}
	if err := configure(ctx, n); err != nil {
		return nil, err
	}
	blockService, err := bserv.New(components.Blockstore(), components.Exchange())
	if err != nil {
		return nil, err
	}
	dag := merkledag.NewDAGService(blockService)
	pinner, err := pin.LoadPinner(components.Datastore(), dag)
	if err != nil {
		// TODO what the fuck.
		pinner = pin.NewPinner(components.Datastore(), dag)
	}
	node := &IpfsNode{
		onlineMode:   components.OnlineMode(),
		Peerstore:    components.Peerstore(),
		Exchange:     components.Exchange(),
		Blockservice: blockService,
		DAG:          dag,
		Routing:      components.Routing(),
		PeerHost:     components.Host(),
		Resolver:     &path.Resolver{DAG: dag},
		Pinning:      pinner,
		Datastore:    components.Datastore(),
		PrivateKey:   components.PrivateKey(),
		// NB: config.Config is omitted
		components: components,
	}
	ctxg.SetTeardown(node.teardown)
	success = true
	return node, nil
}

func (n *IpfsNode) teardown() error {
	if err := n.Datastore.Close(); err != nil {
		return err
	}
	return nil
}

func (n *IpfsNode) OnlineMode() bool {
	return n.onlineMode
}

func (c *IpfsNode) ID() peer.ID {
	return c.components.ID()
}

func (c *IpfsNode) Bootstrap(ctx context.Context, p peer.ID) error {
	return c.components.Bootstrap(ctx, p)
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
