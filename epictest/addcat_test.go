package epictest

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"testing"
	"time"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	random "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-random"
	core "github.com/jbenet/go-ipfs/core"
	core_testutil "github.com/jbenet/go-ipfs/core/testutil"
	mocknet "github.com/jbenet/go-ipfs/p2p/net/mock"
	errors "github.com/jbenet/go-ipfs/util/debugerror"
)

const kSeed = 1

func Test1KBInstantaneous(t *testing.T) {
	conf := core_testutil.LatencyConfig{
		NetworkLatency:    0,
		RoutingLatency:    0,
		BlockstoreLatency: 0,
	}

	if err := DirectAddCat(RandomBytes(1*KB), conf); err != nil {
		t.Fatal(err)
	}
}

func TestDegenerateSlowBlockstore(t *testing.T) {
	SkipUnlessEpic(t)
	conf := core_testutil.LatencyConfig{BlockstoreLatency: 50 * time.Millisecond}
	if err := AddCatPowers(conf, 128); err != nil {
		t.Fatal(err)
	}
}

func TestDegenerateSlowNetwork(t *testing.T) {
	SkipUnlessEpic(t)
	conf := core_testutil.LatencyConfig{NetworkLatency: 400 * time.Millisecond}
	if err := AddCatPowers(conf, 128); err != nil {
		t.Fatal(err)
	}
}

func TestDegenerateSlowRouting(t *testing.T) {
	SkipUnlessEpic(t)
	conf := core_testutil.LatencyConfig{RoutingLatency: 400 * time.Millisecond}
	if err := AddCatPowers(conf, 128); err != nil {
		t.Fatal(err)
	}
}

func Test100MBMacbookCoastToCoast(t *testing.T) {
	SkipUnlessEpic(t)
	conf := core_testutil.LatencyConfig{}.Network_NYtoSF().Blockstore_SlowSSD2014().Routing_Slow()
	if err := DirectAddCat(RandomBytes(100*1024*1024), conf); err != nil {
		t.Fatal(err)
	}
}

func AddCatPowers(conf core_testutil.LatencyConfig, megabytesMax int64) error {
	var i int64
	for i = 1; i < megabytesMax; i = i * 2 {
		fmt.Printf("%d MB\n", i)
		if err := DirectAddCat(RandomBytes(i*1024*1024), conf); err != nil {
			return err
		}
	}
	return nil
}

func RandomBytes(n int64) []byte {
	var data bytes.Buffer
	random.WritePseudoRandomBytes(n, &data, kSeed)
	return data.Bytes()
}

func DirectAddCat(data []byte, conf core_testutil.LatencyConfig) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const numPeers = 2

	// create network
	mn, err := mocknet.FullMeshLinked(ctx, numPeers)
	if err != nil {
		return errors.Wrap(err)
	}
	mn.SetLinkDefaults(mocknet.LinkOptions{
		Latency: conf.NetworkLatency,
		// TODO add to conf. This is tricky because we want 0 values to be functional.
		Bandwidth: math.MaxInt32,
	})

	peers := mn.Peers()
	if len(peers) < numPeers {
		return errors.New("test initialization error")
	}

	adder, err := core.MakeCore(ctx, core.MocknetTestRepo(peers[0], mn.Host(peers[0]), conf))
	if err != nil {
		return err
	}
	catter, err := core.MakeCore(ctx, core.MocknetTestRepo(peers[1], mn.Host(peers[1]), conf))
	if err != nil {
		return err
	}

	adder.Bootstrap(ctx, catter.ID())
	catter.Bootstrap(ctx, adder.ID())

	keyAdded, err := adder.Add(bytes.NewReader(data))
	if err != nil {
		return err
	}

	readerCatted, err := catter.Cat(keyAdded)
	if err != nil {
		return err
	}

	// verify
	var bufout bytes.Buffer
	io.Copy(&bufout, readerCatted)
	if 0 != bytes.Compare(bufout.Bytes(), data) {
		return errors.New("catted data does not match added data")
	}
	return nil
}

func SkipUnlessEpic(t *testing.T) {
	if os.Getenv("IPFS_EPIC_TEST") == "" {
		t.SkipNow()
	}
}
