package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	ci "github.com/ipfs/go-ipfs/p2p/crypto"
	peer "github.com/ipfs/go-ipfs/p2p/peer"
)

func Init(out io.Writer, nBitsForKeypair int, datastore string) (*Config, error) {
	identity, err := identityConfig(out, nBitsForKeypair)
	if err != nil {
		return nil, err
	}

	bootstrapPeers, err := DefaultBootstrapPeers()
	if err != nil {
		return nil, err
	}

	snr, err := initSNRConfig()
	if err != nil {
		return nil, err
	}

	ds, err := datastoreConfig("", datastore)
	if err != nil {
		return nil, err
	}

	conf := &Config{

		// setup the node's default addresses.
		// Note: two swarm listen addrs, one tcp, one utp.
		Addresses: Addresses{
			Swarm: []string{
				"/ip4/0.0.0.0/tcp/4001",
				// "/ip4/0.0.0.0/udp/4002/utp", // disabled for now.
				"/ip6/::/tcp/4001",
			},
			API:     "/ip4/127.0.0.1/tcp/5001",
			Gateway: "/ip4/127.0.0.1/tcp/8080",
		},

		Bootstrap:        BootstrapPeerStrings(bootstrapPeers),
		SupernodeRouting: *snr,
		Identity:         identity,
		Discovery: Discovery{MDNS{
			Enabled:  true,
			Interval: 10,
		}},
		Log: Log{
			MaxSizeMB:  250,
			MaxBackups: 1,
		},

		// setup the node mount points.
		Mounts: Mounts{
			IPFS: "/ipfs",
			IPNS: "/ipns",
		},

		Ipns: Ipns{
			ResolveCacheSize: 128,
		},

		Datastore: *ds,

		// tracking ipfs version used to generate the init folder and adding
		// update checker default setting.
		Version: VersionDefaultValue(),

		Gateway: Gateway{
			RootRedirect: "",
			Writable:     false,
		},
	}

	return conf, nil
}

func datastoreConfig(configroot string, datastore string) (*Datastore, error) {
	dspath, err := DataStorePath(configroot, datastore)
	if err != nil {
		return nil, err
	}

	if len(datastore) != 0 {
		dspath = datastore
	}

	return &Datastore{
		Path:               dspath,
		Type:               "leveldb",
		StorageMax:         "10GB",
		StorageGCWatermark: 90, // 90%
		GCPeriod:           "1h",
	}, nil
}

// identityConfig initializes a new identity.
func identityConfig(out io.Writer, nbits int) (Identity, error) {
	// TODO guard higher up
	ident := Identity{}
	if nbits < 1024 {
		return ident, errors.New("Bitsize less than 1024 is considered unsafe.")
	}

	fmt.Fprintf(out, "generating %v-bit RSA keypair...", nbits)
	sk, pk, err := ci.GenerateKeyPair(ci.RSA, nbits)
	if err != nil {
		return ident, err
	}
	fmt.Fprintf(out, "done\n")

	// currently storing key unencrypted. in the future we need to encrypt it.
	// TODO(security)
	skbytes, err := sk.Bytes()
	if err != nil {
		return ident, err
	}
	ident.PrivKey = base64.StdEncoding.EncodeToString(skbytes)

	id, err := peer.IDFromPublicKey(pk)
	if err != nil {
		return ident, err
	}
	ident.PeerID = id.Pretty()
	fmt.Fprintf(out, "peer identity: %s\n", ident.PeerID)
	return ident, nil
}
