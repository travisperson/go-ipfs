// package blockservice implements a BlockService interface that provides
// a single GetBlock/AddBlock interface that seamlessly retrieves data either
// locally or from a remote peer through the exchange.
package blockservice

import (
	"errors"
	"fmt"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	process "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/goprocess"
	procrl "github.com/jbenet/go-ipfs/Godeps/_workspace/src/github.com/jbenet/goprocess/ratelimit"
	blocks "github.com/jbenet/go-ipfs/blocks"
	"github.com/jbenet/go-ipfs/blocks/blockstore"
	exchange "github.com/jbenet/go-ipfs/exchange"
	u "github.com/jbenet/go-ipfs/util"
)

var log = u.Logger("blockservice")
var ErrNotFound = errors.New("blockservice: key not found")

// MaxExchangeAddWorkers rate limits the number of exchange workers
var MaxExchangeAddWorkers = 100

// BlockService is a hybrid block datastore. It stores data in a local
// datastore and may retrieve data from a remote Exchange.
// It uses an internal `datastore.Datastore` instance to store values.
type BlockService struct {
	// TODO don't expose underlying impl details
	Blockstore blockstore.Blockstore
	Exchange   exchange.Interface

	rateLimiter *procrl.RateLimiter
	exchangeAdd chan blocks.Block
}

// NewBlockService creates a BlockService with given datastore instance.
func New(bs blockstore.Blockstore, rem exchange.Interface) (*BlockService, error) {
	if bs == nil {
		return nil, fmt.Errorf("BlockService requires valid blockstore")
	}
	if rem == nil {
		log.Warning("blockservice running in local (offline) mode.")
	}

	// exchangeAdd is a channel for async workers to add to the exchange.
	// 100 blocks buffer. not clear what this number should be
	exchangeAdd := make(chan blocks.Block, 100)

	return &BlockService{
		Blockstore:  bs,
		Exchange:    rem,
		exchangeAdd: exchangeAdd,
		rateLimiter: procrl.NewRateLimiter(process.Background(), MaxExchangeAddWorkers),
	}, nil
}

// AddBlock adds a particular block to the service, Putting it into the datastore.
// TODO pass a context into this if the remote.HasBlock is going to remain here.
func (s *BlockService) AddBlock(b *blocks.Block) (u.Key, error) {
	k := b.Key()
	err := s.Blockstore.Put(b)
	if err != nil {
		return k, err
	}

	// this operation rate-limits blockservice operations, so it is
	// now an async process.
	if s.Exchange != nil {

		// LimitedGo will spawn a goroutine but provide proper backpressure.
		// it will not spawn the goroutine until the ratelimiter's work load
		// is under the threshold.
		s.rateLimiter.LimitedGo(func(worker process.Process) {
			ctx := context.TODO()
			if err := s.Exchange.HasBlock(ctx, b); err != nil {
				// suppress error, as the client shouldn't care about bitswap.
				// the client only cares about the blockstore.Put.
				log.Errorf("Exchange.HasBlock error: %s", err)
			}
		})
	}
	return k, nil
}

// GetBlock retrieves a particular block from the service,
// Getting it from the datastore using the key (hash).
func (s *BlockService) GetBlock(ctx context.Context, k u.Key) (*blocks.Block, error) {
	log.Debugf("BlockService GetBlock: '%s'", k)
	block, err := s.Blockstore.Get(k)
	if err == nil {
		return block, nil
		// TODO be careful checking ErrNotFound. If the underlying
		// implementation changes, this will break.
	} else if err == blockstore.ErrNotFound && s.Exchange != nil {
		log.Debug("Blockservice: Searching bitswap.")
		blk, err := s.Exchange.GetBlock(ctx, k)
		if err != nil {
			return nil, err
		}
		return blk, nil
	} else {
		log.Debug("Blockservice GetBlock: Not found.")
		return nil, ErrNotFound
	}
}

// GetBlocks gets a list of blocks asynchronously and returns through
// the returned channel.
// NB: No guarantees are made about order.
func (s *BlockService) GetBlocks(ctx context.Context, ks []u.Key) <-chan *blocks.Block {
	out := make(chan *blocks.Block, 0)
	go func() {
		defer close(out)
		var misses []u.Key
		for _, k := range ks {
			hit, err := s.Blockstore.Get(k)
			if err != nil {
				misses = append(misses, k)
				continue
			}
			log.Debug("Blockservice: Got data in datastore.")
			select {
			case out <- hit:
			case <-ctx.Done():
				return
			}
		}

		rblocks, err := s.Exchange.GetBlocks(ctx, misses)
		if err != nil {
			log.Errorf("Error with GetBlocks: %s", err)
			return
		}

		for b := range rblocks {
			select {
			case out <- b:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// DeleteBlock deletes a block in the blockservice from the datastore
func (s *BlockService) DeleteBlock(k u.Key) error {
	return s.Blockstore.DeleteBlock(k)
}
