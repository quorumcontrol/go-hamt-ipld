package hamt

import (
	"github.com/ipfs/go-ipld-format"
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"

	"github.com/ipfs/go-blockservice"
	datastore "github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
)

func MemoryStore(ctx context.Context) (format.DAGService, error) {
	store := dsync.MutexWrap(datastore.NewMapDatastore())
	return FromDatastoreOffline(ctx, store)
}

func MustMemoryStore() format.DAGService {
	ds, err := MemoryStore(context.Background())
	if err != nil {
		panic(err)
	}
	return ds
}

func FromDatastoreOffline(ctx context.Context, ds datastore.Batching) (format.DAGService, error) {
	bs := blockstore.NewBlockstore(ds)
	bs = blockstore.NewIdStore(bs)
	cachedbs, err := blockstore.CachedBlockstore(ctx, bs, blockstore.DefaultCacheOpts())
	if err != nil {
		return nil, err
	}

	bserv := blockservice.New(cachedbs, &nullExchange{}) //only do offline for now.

	dags := merkledag.NewDAGService(bserv)
	return dags, nil
}

type nullExchange struct {
	exchange.Interface
}

func (ne *nullExchange) HasBlock(_ blocks.Block) error {
	return nil
}

func (ne *nullExchange) IsOnline() bool {
	return false
}

func (ne *nullExchange) GetBlock(context.Context, cid.Cid) (blocks.Block, error) {
	return nil, blockstore.ErrNotFound
}
