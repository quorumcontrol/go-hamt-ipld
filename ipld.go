package hamt

import (
	"context"
	"time"

	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"

	cbor "github.com/ipfs/go-ipld-cbor"
	atlas "github.com/polydawn/refmt/obj/atlas"

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
	Atlas *atlas.Atlas
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
		Nodes: dagservice,
	}
}

func (s *CborIpldStore) Get(ctx context.Context, c cid.Cid, out interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	blk, err := s.Nodes.Get(ctx, c)
	if err != nil {
		return err
	}

	return cbor.DecodeInto(blk.RawData(), out)
}

func (s *CborIpldStore) Put(ctx context.Context, v interface{}) (cid.Cid, error) {

	nd, err := cbor.WrapObject(v, multihash.SHA2_256, -1)
	if err != nil {
		return cid.Cid{}, err
	}

	if err := s.Nodes.Add(ctx, nd); err != nil {
		return cid.Cid{}, err
	}

	return nd.Cid(), nil
}
