package network

import (
	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"

	bsmsg "github.com/jbenet/go-ipfs/exchange/bitswap/message"
	peer "github.com/jbenet/go-ipfs/p2p/peer"
	protocol "github.com/jbenet/go-ipfs/p2p/protocol"
	u "github.com/jbenet/go-ipfs/util"
)

var ProtocolBitswap protocol.ID = "/ipfs/bitswap"

// BitSwapNetwork provides network connectivity for BitSwap sessions
type BitSwapNetwork interface {

	// SendMessage sends a BitSwap message to a peer.
	SendMessage(
		context.Context,
		peer.ID,
		bsmsg.BitSwapMessage) error

	// SendRequest sends a BitSwap message to a peer and waits for a response.
	SendRequest(
		context.Context,
		peer.ID,
		bsmsg.BitSwapMessage) (incoming bsmsg.BitSwapMessage, err error)

	// SetDelegate registers the Reciver to handle messages received from the
	// network.
	SetDelegate(Receiver)

	Routing
}

// Implement Receiver to receive messages from the BitSwapNetwork
type Receiver interface {
	ReceiveMessage(
		ctx context.Context,
		sender peer.ID,
		incoming bsmsg.BitSwapMessage,
	) (destination peer.ID, outgoing bsmsg.BitSwapMessage)
}

// The ReceiverFunc type is an adapter to allow the use of ordinary functions
// as Receivers. If f is a function with the appropriate signature,
// ReceiverFunc(f) is a Receiver object that calls f.
type ReceiverFunc func(
	ctx context.Context,
	sender peer.ID,
	incoming bsmsg.BitSwapMessage) (destination peer.ID, outgoing bsmsg.BitSwapMessage)

// ReceiveMessage calls f(ctx, sender, incoming)
func (f ReceiverFunc) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming bsmsg.BitSwapMessage) (destination peer.ID, outgoing bsmsg.BitSwapMessage) {
	return f(ctx, sender, incoming)
}

type Routing interface {
	// FindProvidersAsync returns a channel of providers for the given key
	FindProvidersAsync(context.Context, u.Key, int) <-chan peer.ID

	// Provide provides the key to the network
	Provide(context.Context, u.Key) error
}
