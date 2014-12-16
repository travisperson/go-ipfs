package grandcentral

import (
	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	proto "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/goprotobuf/proto"
	datastore "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore"
	inet "github.com/jbenet/go-ipfs/net"
	peer "github.com/jbenet/go-ipfs/peer"
	dhtpb "github.com/jbenet/go-ipfs/routing/dht/pb"
	proxy "github.com/jbenet/go-ipfs/routing/grandcentral/proxy"
	util "github.com/jbenet/go-ipfs/util"
	errors "github.com/jbenet/go-ipfs/util/debugerror"
)

// Server handles routing queries using a database backend
type Server struct {
	local           peer.Peer
	datastore       datastore.ThreadSafeDatastore
	dialer          inet.Dialer
	peerstore       peer.Peerstore
	*proxy.Loopback // so server can be injected into client
}

// NewServer creates a new GrandCentral routing Server
func NewServer(ds datastore.ThreadSafeDatastore, d inet.Dialer, ps peer.Peerstore, local peer.Peer) (*Server, error) {
	s := &Server{local, ds, d, ps, nil}
	s.Loopback = &proxy.Loopback{
		Handler: s,
		Local:   local,
	}
	return s, nil
}

// HandleLocalRequest implements the proxy.RequestHandler interface. This is
// where requests are received from the outside world.
func (s *Server) HandleRequest(ctx context.Context, p peer.Peer, req *dhtpb.Message) *dhtpb.Message {
	_, response := s.handleMessage(ctx, p, req) // ignore response peer. it's local.
	return response
}

// TODO extract backend. backend can be implemented with whatever database we desire
func (s *Server) handleMessage(
	ctx context.Context, p peer.Peer, req *dhtpb.Message) (peer.Peer, *dhtpb.Message) {

	//  FIXME threw everything into this switch statement to get things going.
	//  Once each operation is well-defined, extract pluggable backend so any
	//  database may be used.

	var response = dhtpb.NewMessage(req.GetType(), req.GetKey(), req.GetClusterLevel())
	switch req.GetType() {

	case dhtpb.Message_GET_VALUE:
		dskey := util.Key(req.GetKey()).DsKey()
		val, err := s.datastore.Get(dskey)
		if err != nil {
			log.Error(errors.Wrap(err))
			return nil, nil
		}
		rawRecord, ok := val.([]byte)
		if !ok {
			log.Errorf("datastore had non byte-slice value for %v", dskey)
			return nil, nil
		}
		if err := proto.Unmarshal(rawRecord, response.Record); err != nil {
			log.Error("failed to unmarshal dht record from datastore")
			return nil, nil
		}
		// TODO before merging: if we know any providers for the requested value, return those.
		return p, response

	case dhtpb.Message_PUT_VALUE:
		// TODO before merging: verifyRecord(req.GetRecord())
		data, err := proto.Marshal(req.GetRecord())
		if err != nil {
			log.Error(err)
			return nil, nil
		}
		dskey := util.Key(req.GetKey()).DsKey()
		if err := s.datastore.Put(dskey, data); err != nil {
			log.Error(err)
			return nil, nil
		}
		return p, req // TODO before merging: verify that we should return record

	case dhtpb.Message_FIND_NODE:
		p, err := s.peerstore.Get(peer.ID(req.GetKey()))
		if err != nil {
			return nil, nil
		}
		response.CloserPeers = dhtpb.PeersToPBPeers(s.dialer, []peer.Peer{p})
		return p, response

	case dhtpb.Message_ADD_PROVIDER:
		for _, provider := range req.GetProviderPeers() {
			providerID := peer.ID(provider.GetId())
			if !providerID.Equal(p.ID()) {
				log.Errorf("provider message came from third-party %s", p)
				continue
			}
			maddrs, err := provider.Addresses()
			if err != nil {
				log.Errorf("failed to extract multiaddrs from message: %s", provider.Addrs)
				continue
			}
			for _, maddr := range maddrs {
				p.AddAddress(maddr)
			}
			// FIXME do we actually want to store to peerstore
			if _, err := s.peerstore.Add(p); err != nil {
				log.Error(errors.Wrap(err))
				return nil, nil
			}
		}
		// TODO store to datastore? what format?
		// key := util.Key(req.GetKey())
		return nil, nil // TODO

	case dhtpb.Message_GET_PROVIDERS:

		// TODO how do we want to persist peers? FIXME along with
		// Message_ADD_PROVIDER.

		return nil, nil // TODO

	case dhtpb.Message_PING:
		return p, req
	default:
	}
	return nil, nil
}

var _ proxy.RequestHandler = &Server{}
var _ proxy.Proxy = &Server{}
