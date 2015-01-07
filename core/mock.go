package core

import (
	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
	core_testutil "github.com/jbenet/go-ipfs/core/testutil"
	mocknet "github.com/jbenet/go-ipfs/p2p/net/mock"
	errors "github.com/jbenet/go-ipfs/util/debugerror"
)

// TODO this is super sketch. Deprecate and initialize one that shares code
// with the actual core constructor. Lots of fields aren't initialized.
// Additionally, the context group isn't wired up. This is as good as broken.

// NewMockNode constructs an IpfsNode for use in tests.
func NewMockNode() (*IpfsNode, error) {
	conf := core_testutil.LatencyConfig{}
	ctx := context.TODO()
	mn, err := mocknet.FullMeshLinked(ctx, 1)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	peers := mn.Peers()
	return New(ctx, MocknetTestRepo(peers[0], mn.Host(peers[0]), conf))
}
