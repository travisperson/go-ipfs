package proxy

import (
	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	proto "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/goprotobuf/proto"
	errors "github.com/jbenet/go-ipfs/util/debugerror"
	netmsg "github.com/jbenet/go-ipfs/net/message"
	dhtpb "github.com/jbenet/go-ipfs/routing/dht/pb"
)

// RequestHandler handles routing requests locally
type RequestHandler interface {
	HandleLocalRequest(ctx context.Context, m *dhtpb.Message) *dhtpb.Message
}

// Loopback forwards requests to a local handler
type Loopback struct {
	Handler RequestHandler
}

// SendMessage intercepts local requests, forwarding them to a local handler
func (lb *Loopback) SendMessage(ctx context.Context, m *dhtpb.Message) error {
	response := lb.Handler.HandleLocalRequest(ctx, m)
	if response != nil {
		log.Warning("loopback handler returned unexpected message")
	}
	return nil
}

// SendRequest intercepts local requests, forwarding them to a local handler
func (lb *Loopback) SendRequest(ctx context.Context, m *dhtpb.Message) (*dhtpb.Message, error) {
	return lb.Handler.HandleLocalRequest(ctx, m), nil
}

func (lb *Loopback) HandleMessage(ctx context.Context, raw netmsg.NetMessage) netmsg.NetMessage {
	var incoming dhtpb.Message // var avoids heap allocation since message doesn't leave this scope
	if err := proto.Unmarshal(raw.Data(), &incoming); err != nil {
		log.Error(errors.Wrap(err))
		return nil
	}
	outgoing := lb.Handler.HandleLocalRequest(ctx, &incoming)
	envelope, err := netmsg.FromObject(raw.Peer(), outgoing)
	if err != nil {
		log.Error(errors.Wrap(err))
		return nil
	}
	return envelope
}
