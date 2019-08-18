package hamt

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	format "github.com/ipfs/go-ipld-format"
	"github.com/quorumcontrol/go-hamt-ipld/goipldpb"

	/*
		bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
				bserv "github.com/ipfs/go-ipfs/blockservice"
				offline "github.com/ipfs/go-ipfs/exchange/offline"
	*/

	cbor "github.com/ipfs/go-ipld-cbor"
	atlas "github.com/polydawn/refmt/obj/atlas"

	//ds "gx/ipfs/QmdHG8MAuARdGHxx4rPQASLcvhz24fzjSQq7AJRAQEorq5/go-datastore"
	cid "github.com/ipfs/go-cid"
)

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
		return fmt.Errorf("error getting: %v", err)
	}

	switch c.Type() {
	case cid.DagProtobuf:
		msg, ok := out.(proto.Message)
		if !ok {
			return fmt.Errorf("could not convert %v into proto.Message", out)
		}
		return goipldpb.DecodeInto(blk.RawData(), msg)
	default:
		return cbor.DecodeInto(blk.RawData(), out)
	}
}

type cidProvider interface {
	Cid() cid.Cid
}

const mhType = uint64(math.MaxUint64)
const mhLen = -1

func (s *CborIpldStore) Put(ctx context.Context, v interface{}) (cid.Cid, error) {
	var nd format.Node
	var err error
	switch msg := v.(type) {
	case proto.Message:
		nd, err = goipldpb.WrapObject(msg)
	default:
		nd, err = WrapObject(v)
		if err != nil {
			return cid.Undef, err
		}
	}
	if err != nil {
		return cid.Undef, err
	}
	if err := s.Nodes.Add(ctx, nd); err != nil {
		return cid.Undef, err
	}

	return nd.Cid(), nil
}

func WrapObject(v interface{}) (format.Node, error) {
	nd, err := cbor.WrapObject(v, mhType, mhLen)
	if err != nil {
		return nil, err
	}
	return nd, nil
}
