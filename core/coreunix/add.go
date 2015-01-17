package coreunix

import (
	"io"

	core "github.com/jbenet/go-ipfs/core"
	importer "github.com/jbenet/go-ipfs/importer"
	chunk "github.com/jbenet/go-ipfs/importer/chunk"
	u "github.com/jbenet/go-ipfs/util"
)

// Add builds a merkledag from the a reader, pinning all objects to the local
// datastore. Returns a key representing the root node.
func Add(n *core.IpfsNode, r io.Reader) (u.Key, error) {
	// TODO more attractive function signature importer.BuildDagFromReader
	dagNode, err := importer.BuildDagFromReader(
		r,
		n.DAG,
		n.Pinning.GetManual(), // Fix this interface
		chunk.DefaultSplitter,
	)
	if err != nil {
		return "", err
	}
	return dagNode.Key()
}
