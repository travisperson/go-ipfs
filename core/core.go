package core

import (
	"fmt"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	b58 "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-base58"
	ctxgroup "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-ctxgroup"
	ma "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-multiaddr"

	bstore "github.com/jbenet/go-ipfs/blocks/blockstore"
	bserv "github.com/jbenet/go-ipfs/blockservice"
	config "github.com/jbenet/go-ipfs/config"
	diag "github.com/jbenet/go-ipfs/diagnostics"
	exchange "github.com/jbenet/go-ipfs/exchange"
	bitswap "github.com/jbenet/go-ipfs/exchange/bitswap"
	bsnet "github.com/jbenet/go-ipfs/exchange/bitswap/network"
	"github.com/jbenet/go-ipfs/exchange/offline"
	mount "github.com/jbenet/go-ipfs/fuse/mount"
	merkledag "github.com/jbenet/go-ipfs/merkledag"
	namesys "github.com/jbenet/go-ipfs/namesys"
	ic "github.com/jbenet/go-ipfs/p2p/crypto"
	p2phost "github.com/jbenet/go-ipfs/p2p/host"
	p2pbhost "github.com/jbenet/go-ipfs/p2p/host/basic"
	swarm "github.com/jbenet/go-ipfs/p2p/net/swarm"
	peer "github.com/jbenet/go-ipfs/p2p/peer"
	path "github.com/jbenet/go-ipfs/path"
	pin "github.com/jbenet/go-ipfs/pin"
	routing "github.com/jbenet/go-ipfs/routing"
	dht "github.com/jbenet/go-ipfs/routing/dht"
	ds2 "github.com/jbenet/go-ipfs/util/datastore2"
	debugerror "github.com/jbenet/go-ipfs/util/debugerror"
	eventlog "github.com/jbenet/go-ipfs/util/eventlog"
)

const IpnsValidatorTag = "ipns"
const kSizeBlockstoreWriteCache = 100

var log = eventlog.Logger("core")

// IpfsNode is IPFS Core module. It represents an IPFS instance.
type IpfsNode struct {

	// Self
	Config     *config.Config // the node's configuration
	Identity   peer.ID        // the local node's identity
	PrivateKey ic.PrivKey     // the local node's private Key
	onlineMode bool           // alternatively, offline

	// Local node
	Datastore ds2.ThreadSafeDatastoreCloser // the local datastore
	Pinning   pin.Pinner                    // the pinning manager
	Mounts    Mounts                        // current mount state, if any.

	// Services
	Peerstore   peer.Peerstore       // storage for other Peer instances
	PeerHost    p2phost.Host         // the network host (server+client)
	Routing     routing.IpfsRouting  // the routing system. recommend ipfs-dht
	Exchange    exchange.Interface   // the block exchange + strategy (bitswap)
	Blockstore  bstore.Blockstore    // the block store (lower level)
	Blocks      *bserv.BlockService  // the block service, get/add blocks.
	DAG         merkledag.DAGService // the merkle dag service, get/add objects.
	Resolver    *path.Resolver       // the path resolution system
	Namesys     namesys.NameSystem   // the name system, resolves paths to hashes
	Diagnostics *diag.Diagnostics    // the diagnostics service

	ctxgroup.ContextGroup
}

// Mounts defines what the node's mount state is. This should
// perhaps be moved to the daemon or mount. It's here because
// it needs to be accessible across daemon requests.
type Mounts struct {
	Ipfs mount.Mount
	Ipns mount.Mount
}

// NewIpfsNode constructs a new IpfsNode based on the given config.
func NewIpfsNode(ctx context.Context, cfg *config.Config, online bool) (n *IpfsNode, err error) {
	success := false // flip to true after all sub-system inits succeed
	defer func() {
		if !success && n != nil {
			n.Close()
		}
	}()

	if cfg == nil {
		return nil, debugerror.Errorf("configuration required")
	}

	n = &IpfsNode{
		onlineMode: online,
		Config:     cfg,
	}
	n.ContextGroup = ctxgroup.WithContextAndTeardown(ctx, n.teardown)
	ctx = n.ContextGroup.Context()

	n.Peerstore = peer.NewPeerstore()

	if n.Datastore, err = makeDatastore(cfg.Datastore); err != nil {
		return nil, debugerror.Wrap(err)
	}

	// setup local peer ID (private key is loaded in online setup)
	if err := n.loadID(); err != nil {
		return nil, err
	}

	n.Blockstore, err = bstore.WriteCached(bstore.NewBlockstore(n.Datastore), kSizeBlockstoreWriteCache)
	if err != nil {
		return nil, debugerror.Wrap(err)
	}

	if online {
		if err := n.StartOnlineServices(); err != nil {
			return nil, err // debugerror.Wraps.
		}
	} else {
		n.Exchange = offline.Exchange(n.Blockstore)
	}

	n.Blocks, err = bserv.New(n.Blockstore, n.Exchange)
	if err != nil {
		return nil, debugerror.Wrap(err)
	}

	n.DAG = merkledag.NewDAGService(n.Blocks)
	n.Pinning, err = pin.LoadPinner(n.Datastore, n.DAG)
	if err != nil {
		n.Pinning = pin.NewPinner(n.Datastore, n.DAG)
	}
	n.Resolver = &path.Resolver{DAG: n.DAG}

	success = true
	return n, nil
}

func (n *IpfsNode) StartOnlineServices() error {
	ctx := n.Context()

	if n.PeerHost != nil { // already online.
		return debugerror.New("node already online")
	}

	// load private key
	if err := n.loadPrivateKey(); err != nil {
		return err
	}

	// setup the network
	listenAddrs, err := listenAddresses(n.Config)
	if err != nil {
		return debugerror.Wrap(err)
	}
	network, err := swarm.NewNetwork(ctx, listenAddrs, n.Identity, n.Peerstore)
	if err != nil {
		return debugerror.Wrap(err)
	}
	n.AddChildGroup(network.CtxGroup())
	n.PeerHost = p2pbhost.New(network)

	// explicitly set these as our listen addrs.
	// (why not do it inside inet.NewNetwork? because this way we can
	// listen on addresses without necessarily advertising those publicly.)
	addrs, err := n.PeerHost.Network().InterfaceListenAddresses()
	if err != nil {
		return debugerror.Wrap(err)
	}
	n.Peerstore.AddAddresses(n.Identity, addrs)

	// setup diagnostics service
	n.Diagnostics = diag.NewDiagnostics(n.Identity, n.PeerHost)

	// setup routing service
	dhtRouting := dht.NewDHT(ctx, n.PeerHost, n.Datastore)
	dhtRouting.Validators[IpnsValidatorTag] = namesys.ValidateIpnsRecord
	n.Routing = dhtRouting
	n.AddChildGroup(dhtRouting)

	// setup exchange service
	const alwaysSendToPeer = true // use YesManStrategy
	bitswapNetwork := bsnet.NewFromIpfsHost(n.PeerHost, n.Routing)
	n.Exchange = bitswap.New(ctx, n.Identity, bitswapNetwork, n.Blockstore, alwaysSendToPeer)

	// setup name system
	// TODO implement an offline namesys that serves only local names.
	n.Namesys = namesys.NewNameSystem(n.Routing)

	// TODO consider moving connection supervision into the Network. We've
	// discussed improvements to this Node constructor. One improvement
	// would be to make the node configurable, allowing clients to inject
	// an Exchange, Network, or Routing component and have the constructor
	// manage the wiring. In that scenario, this dangling function is a bit
	// awkward.
	go superviseConnections(ctx, n.PeerHost, dhtRouting, n.Peerstore, n.Config.Bootstrap)
	return nil
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

func (n *IpfsNode) loadID() error {
	if n.Identity != "" {
		return debugerror.New("identity already loaded")
	}

	cid := n.Config.Identity.PeerID
	if cid == "" {
		return debugerror.New("Identity was not set in config (was ipfs init run?)")
	}
	if len(cid) == 0 {
		return debugerror.New("No peer ID in config! (was ipfs init run?)")
	}

	n.Identity = peer.ID(b58.Decode(cid))
	return nil
}

func (n *IpfsNode) loadPrivateKey() error {
	if n.Identity == "" || n.Peerstore == nil {
		return debugerror.New("loaded private key out of order.")
	}

	if n.PrivateKey != nil {
		return debugerror.New("private key already loaded")
	}

	sk, err := loadPrivateKey(&n.Config.Identity, n.Identity)
	if err != nil {
		return err
	}

	n.PrivateKey = sk
	n.Peerstore.AddPrivKey(n.Identity, n.PrivateKey)
	return nil
}

func loadPrivateKey(cfg *config.Identity, id peer.ID) (ic.PrivKey, error) {
	sk, err := cfg.DecodePrivateKey("passphrase todo!")
	if err != nil {
		return nil, err
	}

	id2, err := peer.IDFromPrivateKey(sk)
	if err != nil {
		return nil, err
	}

	if id2 != id {
		return nil, fmt.Errorf("private key in config does not match id: %s != %s", id, id2)
	}

	return sk, nil
}

func listenAddresses(cfg *config.Config) ([]ma.Multiaddr, error) {

	var err error
	listen := make([]ma.Multiaddr, len(cfg.Addresses.Swarm))
	for i, addr := range cfg.Addresses.Swarm {

		listen[i], err = ma.NewMultiaddr(addr)
		if err != nil {
			return nil, fmt.Errorf("Failure to parse config.Addresses.Swarm[%d]: %s", i, cfg.Addresses.Swarm)
		}
	}

	return listen, nil
}
