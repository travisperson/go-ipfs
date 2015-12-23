package fsrepo

import (
	"fmt"
	"path"

	ds "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore"
	"github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore/flatfs"
	levelds "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore/leveldb"
	"github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore/measure"
	mount "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore/syncmount"
	ldbopts "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/syndtr/goleveldb/leveldb/opt"
	repo "github.com/ipfs/go-ipfs/repo"
	config "github.com/ipfs/go-ipfs/repo/config"
	"github.com/ipfs/go-ipfs/thirdparty/dir"
)

const (
	leveldbDirectory = "datastore"
	flatfsDirectory  = "blocks"
)

func openDefaultDatastore(r *FSRepo) (repo.Datastore, error) {
	dsPath := r.config.Datastore.Path

	if len(dsPath) == 0 {
		dsPath = r.path
	}

	leveldbPath := path.Join(dsPath, leveldbDirectory)
	flatfsPath := path.Join(dsPath, flatfsDirectory)

	// save leveldb reference so it can be neatly closed afterward
	leveldbDS, err := levelds.NewDatastore(leveldbPath, &levelds.Options{
		Compression: ldbopts.NoCompression,
	})

	if err != nil {
		return nil, fmt.Errorf("unable to open leveldb datastore: %v", err)
	}

	// 4TB of 256kB objects ~=17M objects, splitting that 256-way
	// leads to ~66k objects per dir, splitting 256*256-way leads to
	// only 256.
	//
	// The keys seen by the block store have predictable prefixes,
	// including "/" from datastore.Key and 2 bytes from multihash. To
	// reach a uniform 256-way split, we need approximately 4 bytes of
	// prefix.
	syncfs := !r.config.Datastore.NoSync
	blocksDS, err := flatfs.New(flatfsPath, 4, syncfs)
	if err != nil {
		return nil, fmt.Errorf("unable to open flatfs datastore: %v", err)
	}

	// Add our PeerID to metrics paths to keep them unique
	//
	// As some tests just pass a zero-value Config to fsrepo.Init,
	// cope with missing PeerID.
	id := r.config.Identity.PeerID
	if id == "" {
		// the tests pass in a zero Config; cope with it
		id = fmt.Sprintf("uninitialized_%p", r)
	}

	prefix := "fsrepo." + id + ".datastore."
	metricsBlocks := measure.New(prefix+"blocks", blocksDS)
	metricsLevelDB := measure.New(prefix+"leveldb", leveldbDS)
	mountDS := mount.New([]mount.Mount{
		{
			Prefix:    ds.NewKey("/blocks"),
			Datastore: metricsBlocks,
		},
		{
			Prefix:    ds.NewKey("/"),
			Datastore: metricsLevelDB,
		},
	})

	return mountDS, nil
}

func initDefaultDatastore(repoPath string, conf *config.Config) error {
	dsPath := conf.Datastore.Path

	if len(dsPath) == 0 {
		dsPath = repoPath
	}

	// The actual datastore contents are initialized lazily when Opened.
	// During Init, we merely check that the directory is writeable.
	leveldbPath := path.Join(dsPath, leveldbDirectory)
	if err := dir.Writable(leveldbPath); err != nil {
		return fmt.Errorf("datastore: %s", err)
	}

	flatfsPath := path.Join(dsPath, flatfsDirectory)
	if err := dir.Writable(flatfsPath); err != nil {
		return fmt.Errorf("datastore: %s", err)
	}

	return nil
}
