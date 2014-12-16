package proxy

import (
	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	proto "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/goprotobuf/proto"
	inet "github.com/jbenet/go-ipfs/net"
	netmsg "github.com/jbenet/go-ipfs/net/message"
	peer "github.com/jbenet/go-ipfs/peer"
	dhtpb "github.com/jbenet/go-ipfs/routing/dht/pb"
	errors "github.com/jbenet/go-ipfs/util/debugerror"
	eventlog "github.com/jbenet/go-ipfs/util/eventlog"
)

var log = eventlog.Logger("proxy")

type Proxy interface {
	SendMessage(ctx context.Context, m *dhtpb.Message) error
	SendRequest(ctx context.Context, m *dhtpb.Message) (*dhtpb.Message, error)
	HandleMessage(ctx context.Context, raw netmsg.NetMessage) netmsg.NetMessage
}

type standard struct {
	NetMessageSender inet.Sender
	Remote           peer.Peer
}

func Standard(s inet.Sender, remote peer.Peer) Proxy {
	return &standard{s, remote}
}

func (px *standard) SendMessage(ctx context.Context, m *dhtpb.Message) error {
	envelope, err := netmsg.FromObject(px.Remote, m)
	if err != nil {
		return errors.Wrap(err)
	}
	return px.NetMessageSender.SendMessage(ctx, envelope)
}

func (px *standard) SendRequest(ctx context.Context, m *dhtpb.Message) (*dhtpb.Message, error) {
	envelope, err := netmsg.FromObject(px.Remote, m)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	rawResponse, err := px.NetMessageSender.SendRequest(ctx, envelope)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	response := new(dhtpb.Message)
	if err := proto.Unmarshal(rawResponse.Data(), response); err != nil {
		return nil, errors.Wrap(err)
	}
	return response, nil
}

func (lb *standard) HandleMessage(ctx context.Context, raw netmsg.NetMessage) netmsg.NetMessage {
	// ignore messages coming in from network
	return nil // TODO consider forwarding message to host
}

func Func(ctx context.Context, raw netmsg.NetMessage, f func(context.Context, peer.Peer, *dhtpb.Message) (peer.Peer, *dhtpb.Message)) netmsg.NetMessage {
	var incoming dhtpb.Message // var avoids heap allocation since message doesn't leave this scope
	if err := proto.Unmarshal(raw.Data(), &incoming); err != nil {
		log.Error(errors.Wrap(err))
		return nil
	}
	recipient, outgoing := f(ctx, raw.Peer(), &incoming)
	envelope, err := netmsg.FromObject(recipient, outgoing)
	if err != nil {
		log.Error(errors.Wrap(err))
		return nil
	}
	return envelope
}
