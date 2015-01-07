package core

import (
	"errors"
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
	repo Configuration
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
func NewIpfsNode(ctx context.Context, cfg *config.Config, online bool) (*IpfsNode, error) {
	if online {
		return New(ctx, Online(cfg))
	}
	return New(ctx, Offline(cfg))
}

// mock network, dht, bitswap
// p2p network, grandcentral, bitswap
//

func Online(cfg *config.Config) Config {
	// TODO load private key
	return func(ctx context.Context) (Configuration, error) {
		if cfg == nil {
			return nil, debugerror.Errorf("configuration required")
		}
		peerID := peer.ID(b58.Decode(cfg.Identity.PeerID))
		privateKey, err := loadPrivateKey(&cfg.Identity, peerID)
		if err != nil {
			return nil, err
		}
		datastore, err := makeDatastore(cfg.Datastore)
		if err != nil {
			return nil, err
		}
		blockstore, err := bstore.WriteCached(bstore.NewBlockstore(datastore), kSizeBlockstoreWriteCache)
		if err != nil {
			return nil, err
		}

		peerstore := peer.NewPeerstore()
		peerstore.AddPrivKey(peerID, privateKey)
		listenAddrs, err := listenAddresses(cfg)
		if err != nil {
			return nil, debugerror.Wrap(err)
		}
		network, err := swarm.NewNetwork(ctx, listenAddrs, peerID, peerstore)
		if err != nil {
			return nil, debugerror.Wrap(err)
		}
		// TODO consider giving this function a context group so the components can be added n.AddChildGroup(network.CtxGroup())
		peerHost := p2pbhost.New(network)

		// Explicitly set these as our listen addrs.
		// Q: Why not do it inside inet.NewNetwork?
		// A: Because this way we can listen on addresses without necessarily
		//    advertising those publicly.
		addrs, err := peerHost.Network().InterfaceListenAddresses()
		if err != nil {
			return nil, debugerror.Wrap(err)
		}
		peerstore.AddAddresses(peerID, addrs)

		diagnosticService := diag.NewDiagnostics(peerID, peerHost)

		dhtRouting := dht.NewDHT(ctx, peerHost, datastore)
		dhtRouting.Validators[IpnsValidatorTag] = namesys.ValidateIpnsRecord
		// TODO n.AddChildGroup(dhtRouting)

		const alwaysSendToPeer = true // use YesManStrategy
		bitswapNetwork := bsnet.NewFromIpfsHost(peerHost, dhtRouting)
		bitswapExchange := bitswap.New(ctx, peerID, bitswapNetwork, blockstore, alwaysSendToPeer)

		// TODO implement an offline namesys that serves only local names.
		nsys := namesys.NewNameSystem(dhtRouting)

		// TODO consider moving connection supervision into the Network. We've
		// discussed improvements to this Node constructor. One improvement
		// would be to make the node configurable, allowing clients to inject
		// an Exchange, Network, or Routing component and have the constructor
		// manage the wiring. In that scenario, this dangling function is a bit
		// awkward.
		go superviseConnections(ctx, peerHost, dhtRouting, peerstore, cfg.Bootstrap)
		return &configuration{
			online:           true,
			exchange:         bitswapExchange,
			diagnoticService: diagnosticService,
			nameSystem:       nsys,
		}, errors.New("TODO")
	}
}

func Offline(cfg *config.Config) Config {
	// offline exchange
	return func(context.Context) (Configuration, error) {
		if cfg == nil {
			return nil, debugerror.Errorf("configuration required")
		}
		return nil, errors.New("TODO")
	}
}

type Configuration interface {
	ID() peer.ID
	Exchange() exchange.Interface
	OnlineMode() bool
	Peerstore() peer.Peerstore
	Blockstore() bstore.Blockstore
	Datastore() ds2.ThreadSafeDatastoreCloser
	// TODO if configID == "" { return debugerror.New("Identity was not set in config (was ipfs init run?)") }
	// TODO if len(configID) == 0 { return debugerror.New("No peer ID in config! (was ipfs init run?)") }

	Bootstrap(ctx context.Context, peer peer.ID) error
}

func New(parent context.Context, c Config) (*IpfsNode, error) {
	ctxg := ctxgroup.WithContext(parent)
	ctx := ctxg.Context()
	success := false // flip to true after all sub-system inits succeed
	defer func() {
		if !success {
			ctxg.Close()
			// TODO handle close
		}
	}()
	config, err := c(ctx)
	if err != nil {
		return nil, err
	}
	blockService, err := bserv.New(config.Blockstore(), config.Exchange())
	if err != nil {
		return nil, err
	}
	dag := merkledag.NewDAGService(blockService)
	pinner, err := pin.LoadPinner(config.Datastore(), dag)
	if err != nil {
		// TODO what the fuck.
		pinner = pin.NewPinner(config.Datastore(), dag)
	}
	node := &IpfsNode{
		onlineMode: config.OnlineMode(),
		Peerstore:  config.Peerstore(),
		Exchange:   config.Exchange(),
		Blocks:     blockService,
		DAG:        dag,
		Resolver:   &path.Resolver{DAG: dag},
		Pinning:    pinner,
		Datastore:  config.Datastore(),
		// NB: config.Config is omitted
		repo: config,
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
