package core

// important to keep as an interface to allow implementations to vary

import (
	"errors"
	"fmt"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	b58 "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-base58"
	datastore "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore"
	sync "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore/sync"
	ma "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-multiaddr"
	blockstore "github.com/jbenet/go-ipfs/blocks/blockstore"
	"github.com/jbenet/go-ipfs/config"
	core_testutil "github.com/jbenet/go-ipfs/core/testutil"
	diag "github.com/jbenet/go-ipfs/diagnostics"
	exchange "github.com/jbenet/go-ipfs/exchange"
	bitswap "github.com/jbenet/go-ipfs/exchange/bitswap"
	bsnet "github.com/jbenet/go-ipfs/exchange/bitswap/network"
	namesys "github.com/jbenet/go-ipfs/namesys"
	ic "github.com/jbenet/go-ipfs/p2p/crypto"
	host "github.com/jbenet/go-ipfs/p2p/host"
	p2pbhost "github.com/jbenet/go-ipfs/p2p/host/basic"
	"github.com/jbenet/go-ipfs/p2p/net/swarm"
	peer "github.com/jbenet/go-ipfs/p2p/peer"
	dht "github.com/jbenet/go-ipfs/routing/dht"
	"github.com/jbenet/go-ipfs/util/datastore2"
	ds2 "github.com/jbenet/go-ipfs/util/datastore2"
	"github.com/jbenet/go-ipfs/util/debugerror"
	delay "github.com/jbenet/go-ipfs/util/delay"
)

type Config func(context.Context, *IpfsNode) error

func Online(cfg *config.Config) Config {
	// TODO load private key
	return func(ctx context.Context, n *IpfsNode) error {
		if cfg == nil {
			return debugerror.Errorf("configuration required")
		}
		peerID := peer.ID(b58.Decode(cfg.Identity.PeerID))
		privateKey, err := loadPrivateKey(&cfg.Identity, peerID)
		if err != nil {
			return err
		}
		datastore, err := makeDatastore(cfg.Datastore)
		if err != nil {
			return err
		}
		blockstore, err := blockstore.WriteCached(blockstore.NewBlockstore(datastore), kSizeBlockstoreWriteCache)
		if err != nil {
			return err
		}

		peerstore := peer.NewPeerstore()
		peerstore.AddPrivKey(peerID, privateKey)
		listenAddrs, err := listenAddresses(cfg)
		if err != nil {
			return debugerror.Wrap(err)
		}
		network, err := swarm.NewNetwork(ctx, listenAddrs, peerID, peerstore)
		if err != nil {
			return debugerror.Wrap(err)
		}
		// TODO consider giving this function a context group so the components can be added n.AddChildGroup(network.CtxGroup())
		peerHost := p2pbhost.New(network)

		// Explicitly set these as our listen addrs.
		// Q: Why not do it inside inet.NewNetwork?
		// A: Because this way we can listen on addresses without necessarily
		//    advertising those publicly.
		addrs, err := peerHost.Network().InterfaceListenAddresses()
		if err != nil {
			return debugerror.Wrap(err)
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
		return errors.New("TODO")
	}
}

func Offline(cfg *config.Config) Config {
	// offline exchange
	return func(context.Context, *IpfsNode) error {
		if cfg == nil {
			return debugerror.Errorf("configuration required")
		}
		return errors.New("TODO")
	}
}

func MocknetTestRepo(p peer.ID, h host.Host, conf core_testutil.LatencyConfig) Config {
	return func(ctx context.Context, n *IpfsNode) error {
		const kWriteCacheElems = 100
		const alwaysSendToPeer = true
		dsDelay := delay.Fixed(conf.BlockstoreLatency)
		ds := ds2.CloserWrap(sync.MutexWrap(datastore2.WithDelay(datastore.NewMapDatastore(), dsDelay)))

		log.Debugf("MocknetTestRepo: %s %s %s", p, h.ID(), h)
		dhtt := dht.NewDHT(ctx, h, ds)
		bsn := bsnet.NewFromIpfsHost(h, dhtt)
		bstore, err := blockstore.WriteCached(blockstore.NewBlockstore(ds), kWriteCacheElems)
		if err != nil {
			return err
		}
		exch := bitswap.New(ctx, p, bsn, bstore, alwaysSendToPeer)
		// return &configuration{
		// 	bitSwapNetwork: bsn,
		// 	blockstore:     bstore,
		// 	exchange:       exch,
		// 	privateKey:     h.Peerstore().PrivKey(p),
		// 	datastore:      ds,
		// 	host:           h,
		// 	peerstore:      h.Peerstore(),
		// 	dht:            dhtt,
		// 	id:             p,
		return nil
	}
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

	privateKey ic.PrivKey
}

func (c *configuration) PrivateKey() ic.PrivKey                         { return c.privateKey }
func (c *configuration) Bootstrap(ctx context.Context, p peer.ID) error { return c.dht.Connect(ctx, p) }
func (c *configuration) OnlineMode() bool                               { return c.online }
func (c *configuration) Peerstore() peer.Peerstore                      { return c.peerstore }
func (c *configuration) Host() host.Host                                { return c.host }
func (c *configuration) Blockstore() blockstore.Blockstore              { return c.blockstore }
func (c *configuration) Datastore() ds2.ThreadSafeDatastoreCloser       { return c.datastore }
func (c *configuration) Exchange() exchange.Interface                   { return c.exchange }
func (c *configuration) ID() peer.ID                                    { return c.id }
