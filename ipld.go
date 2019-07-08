package hamt

import (
	"github.com/ipfs/go-ipld-format"
	"context"
	"math"
	"time"

	/*
		bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
				bserv "github.com/ipfs/go-ipfs/blockservice"
				offline "github.com/ipfs/go-ipfs/exchange/offline"
	*/

	cbor "github.com/ipfs/go-ipld-cbor"
	recbor "github.com/polydawn/refmt/cbor"
	atlas "github.com/polydawn/refmt/obj/atlas"
	//ds "gx/ipfs/QmdHG8MAuARdGHxx4rPQASLcvhz24fzjSQq7AJRAQEorq5/go-datastore"
	cid "github.com/ipfs/go-cid"
)

// THIS IS ALL TEMPORARY CODE

func init() {
	cbor.RegisterCborType(cbor.BigIntAtlasEntry)
	cbor.RegisterCborType(Node{})
	cbor.RegisterCborType(Pointer{})

	kvAtlasEntry := atlas.BuildEntry(KV{}).Transform().TransformMarshal(
		atlas.MakeMarshalTransformFunc(func(kv KV) ([]interface{}, error) {
			return []interface{}{kv.Key, kv.Value}, nil
		})).TransformUnmarshal(
		atlas.MakeUnmarshalTransformFunc(func(v []interface{}) (KV, error) {
			return KV{
				Key:   v[0].(string),
				Value: v[1],
			}, nil
		})).Complete()
	cbor.RegisterCborType(kvAtlasEntry)
}

type CborIpldStore struct {
	Nodes nodes
	Atlas  *atlas.Atlas
}

type nodes interface {
	Get(context.Context, cid.Cid) (format.Node, error)
	Add(context.Context, format.Node) error
}

func NewCborStore() *CborIpldStore {
	return &CborIpldStore{
		Nodes: MustMemoryStore(),
	}
}


func CSTFromDAG(dagservice format.DAGService) *CborIpldStore {
	return &CborIpldStore{
		Nodes:dagservice,
	}
}

func (s *CborIpldStore) Get(ctx context.Context, c cid.Cid, out interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	blk, err := s.Nodes.Get(ctx, c)
	if err != nil {
		return err
	}

	if s.Atlas == nil {
		return cbor.DecodeInto(blk.RawData(), out)
	} else {
		return recbor.UnmarshalAtlased(recbor.DecodeOptions{}, blk.RawData(), out, *s.Atlas)
	}
}

type cidProvider interface {
	Cid() cid.Cid
}

func (s *CborIpldStore) Put(ctx context.Context, v interface{}) (cid.Cid, error) {
	mhType := uint64(math.MaxUint64)
	mhLen := -1
	if c, ok := v.(cidProvider); ok {
		pref := c.Cid().Prefix()
		mhType = pref.MhType
		mhLen = pref.MhLength
	}

	nd, err := cbor.WrapObject(v, mhType, mhLen)
	if err != nil {
		return cid.Cid{}, err
	}

	if err := s.Nodes.Add(ctx, nd); err != nil {
		return cid.Cid{}, err
	}

	return nd.Cid(), nil
}
