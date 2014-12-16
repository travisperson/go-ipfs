package grandcentral

import (
	"bytes"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	proto "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/goprotobuf/proto"
	inet "github.com/jbenet/go-ipfs/net"
	peer "github.com/jbenet/go-ipfs/peer"
	routing "github.com/jbenet/go-ipfs/routing"
	pb "github.com/jbenet/go-ipfs/routing/dht/pb"
	proxy "github.com/jbenet/go-ipfs/routing/grandcentral/proxy"
	u "github.com/jbenet/go-ipfs/util"
	errors "github.com/jbenet/go-ipfs/util/debugerror"
	eventlog "github.com/jbenet/go-ipfs/util/eventlog"
)

var log = eventlog.Logger("grandcentral")

var ErrTODO = errors.New("TODO")

type Client struct {
	peerstore peer.Peerstore
	proxy     proxy.Proxy
	dialer    inet.Dialer
	local     peer.Peer
}

// TODO take in datastore/cache
func NewClient(d inet.Dialer, px proxy.Proxy, ps peer.Peerstore, local peer.Peer) (*Client, error) {
	return &Client{
		dialer:    d,
		proxy:     px,
		local:     local,
		peerstore: ps,
	}, nil
}

func (c *Client) FindProvidersAsync(ctx context.Context, k u.Key, max int) <-chan peer.Peer {
	ch := make(chan peer.Peer)
	go func() {
		defer close(ch)
		request := pb.NewMessage(pb.Message_GET_PROVIDERS, string(k), 0)
		response, err := c.proxy.SendRequest(ctx, request)
		if err != nil {
			log.Error(errors.Wrap(err))
			return
		}
		providers, errs := pb.PBPeersToPeers(c.peerstore, response.GetProviderPeers())
		for _, err := range errs {
			if err != nil {
				log.Error(errors.Wrap(err))
			}
		}
		for _, p := range providers {
			select {
			case <-ctx.Done():
				log.Error(errors.Wrap(ctx.Err()))
				return
			case ch <- p:
			}
		}
	}()
	return ch
}

func (c *Client) PutValue(ctx context.Context, k u.Key, v []byte) error {
	r, err := makeRecord(c.local, k, v)
	if err != nil {
		return err
	}
	pmes := pb.NewMessage(pb.Message_PUT_VALUE, string(k), 0)
	pmes.Record = r
	return c.proxy.SendMessage(ctx, pmes) // wrap to hide the remote
}

func (c *Client) GetValue(ctx context.Context, k u.Key) ([]byte, error) {
	msg := pb.NewMessage(pb.Message_GET_VALUE, string(k), 0)
	response, err := c.proxy.SendRequest(ctx, msg) // TODO wrap to hide the remote
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return response.Record.GetValue(), nil
}

func (c *Client) Provide(ctx context.Context, k u.Key) error {
	msg := pb.NewMessage(pb.Message_ADD_PROVIDER, string(k), 0)
	// TODO wrap this to hide the dialer and the local/remote peers
	msg.ProviderPeers = pb.PeersToPBPeers(c.dialer, []peer.Peer{c.local}) // FIXME how is connectedness defined for the local node
	return c.proxy.SendMessage(ctx, msg)                                  // TODO wrap to hide remote
}

func (c *Client) FindPeer(ctx context.Context, id peer.ID) (peer.Peer, error) {
	request := pb.NewMessage(pb.Message_FIND_NODE, string(id), 0)
	response, err := c.proxy.SendRequest(ctx, request) // hide remote
	if err != nil {
		return nil, errors.Wrap(err)
	}
	potentials, errs := pb.PBPeersToPeers(c.peerstore, response.GetCloserPeers())
	for _, err := range errs {
		if err != nil {
			return nil, errors.Wrap(err) // consider being less strict
		}
	}
	for _, p := range potentials {
		if string(p.ID()) == string(id) {
			return p, nil
		}
	}
	return nil, errors.New("could not find peer")
}

// creates and signs a record for the given key/value pair
func makeRecord(p peer.Peer, k u.Key, v []byte) (*pb.Record, error) {
	blob := bytes.Join([][]byte{[]byte(k), v, []byte(p.ID())}, []byte{})
	sig, err := p.PrivKey().Sign(blob)
	if err != nil {
		return nil, err
	}
	return &pb.Record{
		Key:       proto.String(string(k)),
		Value:     v,
		Author:    proto.String(string(p.ID())),
		Signature: sig,
	}, nil
}

var _ routing.IpfsRouting = &Client{}
