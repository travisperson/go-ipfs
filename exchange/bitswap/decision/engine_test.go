package decision

import (
	"math"
	"strings"
	"sync"
	"testing"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	ds "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore"
	dssync "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore/sync"
	blocks "github.com/jbenet/go-ipfs/blocks"
	blockstore "github.com/jbenet/go-ipfs/blocks/blockstore"
	message "github.com/jbenet/go-ipfs/exchange/bitswap/message"
	peer "github.com/jbenet/go-ipfs/p2p/peer"
	testutil "github.com/jbenet/go-ipfs/util/testutil"
)

type peerAndEngine struct {
	Peer   peer.ID
	Engine *Engine
}

func newEngine(ctx context.Context, idStr string) peerAndEngine {
	return peerAndEngine{
		Peer: peer.ID(idStr),
		//Strategy: New(true),
		Engine: NewEngine(ctx,
			blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))),
	}
}

func TestConsistentAccounting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := newEngine(ctx, "Ernie")
	receiver := newEngine(ctx, "Bert")

	// Send messages from Ernie to Bert
	for i := 0; i < 1000; i++ {

		m := message.New()
		content := []string{"this", "is", "message", "i"}
		m.AddBlock(blocks.NewBlock([]byte(strings.Join(content, " "))))

		sender.Engine.MessageSent(receiver.Peer, m)
		receiver.Engine.MessageReceived(sender.Peer, m)
	}

	// Ensure sender records the change
	if sender.Engine.numBytesSentTo(receiver.Peer) == 0 {
		t.Fatal("Sent bytes were not recorded")
	}

	// Ensure sender and receiver have the same values
	if sender.Engine.numBytesSentTo(receiver.Peer) != receiver.Engine.numBytesReceivedFrom(sender.Peer) {
		t.Fatal("Inconsistent book-keeping. Strategies don't agree")
	}

	// Ensure sender didn't record receving anything. And that the receiver
	// didn't record sending anything
	if receiver.Engine.numBytesSentTo(sender.Peer) != 0 || sender.Engine.numBytesReceivedFrom(receiver.Peer) != 0 {
		t.Fatal("Bert didn't send bytes to Ernie")
	}
}

func TestPeerIsAddedToPeersWhenMessageReceivedOrSent(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sanfrancisco := newEngine(ctx, "sf")
	seattle := newEngine(ctx, "sea")

	m := message.New()

	sanfrancisco.Engine.MessageSent(seattle.Peer, m)
	seattle.Engine.MessageReceived(sanfrancisco.Peer, m)

	if seattle.Peer == sanfrancisco.Peer {
		t.Fatal("Sanity Check: Peers have same Key!")
	}

	if !peerIsPartner(seattle.Peer, sanfrancisco.Engine) {
		t.Fatal("Peer wasn't added as a Partner")
	}

	if !peerIsPartner(sanfrancisco.Peer, seattle.Engine) {
		t.Fatal("Peer wasn't added as a Partner")
	}
}

func peerIsPartner(p peer.ID, e *Engine) bool {
	for _, partner := range e.Peers() {
		if partner == p {
			return true
		}
	}
	return false
}

func TestOutboxClosedWhenEngineClosed(t *testing.T) {
	t.SkipNow() // TODO implement *Engine.Close
	e := NewEngine(context.Background(), blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for _ = range e.Outbox() {
		}
		wg.Done()
	}()
	// e.Close()
	wg.Wait()
	if _, ok := <-e.Outbox(); ok {
		t.Fatal("channel should be closed")
	}
}

func TestPartnerWantsThenCancels(t *testing.T) {
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")
	vowels := strings.Split("aeiou", "")

	type testCase [][]string
	testcases := []testCase{
		testCase{
			alphabet, vowels,
		},
		testCase{
			alphabet, stringsComplement(alphabet, vowels),
		},
	}

	for _, testcase := range testcases {
		set := testcase[0]
		cancels := testcase[1]
		keeps := stringsComplement(set, cancels)

		bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
		e := NewEngine(context.Background(), bs)
		partner := testutil.RandPeerIDFatal(t)
		for _, letter := range set {
			block := blocks.NewBlock([]byte(letter))
			bs.Put(block)
		}
		partnerWants(e, set, partner)
		partnerCancels(e, cancels, partner)
		assertPoppedInOrder(t, e, keeps)
	}

}

func partnerWants(e *Engine, keys []string, partner peer.ID) {
	add := message.New()
	for i, letter := range keys {
		block := blocks.NewBlock([]byte(letter))
		add.AddEntry(block.Key(), math.MaxInt32-i)
	}
	e.MessageReceived(partner, add)
}

func partnerCancels(e *Engine, keys []string, partner peer.ID) {
	cancels := message.New()
	for _, k := range keys {
		block := blocks.NewBlock([]byte(k))
		cancels.Cancel(block.Key())
	}
	e.MessageReceived(partner, cancels)
}

func assertPoppedInOrder(t *testing.T, e *Engine, keys []string) {
	for _, k := range keys {
		envelope := <-e.Outbox()
		received := envelope.Message.Blocks()[0]
		expected := blocks.NewBlock([]byte(k))
		if received.Key() != expected.Key() {
			t.Fatal("received", string(received.Data), "expected", string(expected.Data))
		}
	}
}

func stringsComplement(set, subset []string) []string {
	m := make(map[string]struct{})
	for _, letter := range subset {
		m[letter] = struct{}{}
	}
	var complement []string
	for _, letter := range set {
		if _, exists := m[letter]; !exists {
			complement = append(complement, letter)
		}
	}
	return complement
}
