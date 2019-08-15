package hamt

import (
	"context"
	"math"
	"time"

	format "github.com/ipfs/go-ipld-format"

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
	cbor.RegisterCborType(KV{})
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
	nd, err := WrapObject(v)
	if err != nil {
		return cid.Undef, err
	}
	if err := s.Nodes.Add(ctx, nd); err != nil {
		return cid.Undef, err
	}

	return nd.Cid(), nil
}

const mhType = uint64(math.MaxUint64)
const mhLen = -1

func WrapObject(v interface{}) (format.Node, error) {
	nd, err := cbor.WrapObject(v, mhType, mhLen)
	if err != nil {
		return nil, err
	}
	return nd, nil
}
