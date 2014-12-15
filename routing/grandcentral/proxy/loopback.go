package proxy

import (
	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
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
