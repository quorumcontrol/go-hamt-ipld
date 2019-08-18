package goipldpb

import (
	"fmt"
	"math"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	node "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"

	"github.com/gogo/protobuf/proto"
)

var ErrUnimplemented = fmt.Errorf("unimplemented")

type Node struct {
	blocks.Block
}

func WrapObject(msg proto.Marshaler, mhType uint64, mhLen int) (format.Node, error) {
	data, err := msg.Marshal()
	if err != nil {
		return nil, err
	}

	if mhType == math.MaxUint64 {
		mhType = mh.SHA2_256
	}

	hash, err := mh.Sum(data, mhType, mhLen)
	if err != nil {
		return nil, err
	}
	c := cid.NewCidV1(cid.DagCBOR, hash)

	block, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		// TODO: Shouldn't this just panic?
		return nil, err
	}

	return &Node{
		Block: block,
	}, nil
}

func DecodeInto(bits []byte, out proto.Unmarshaler) error {
	return out.Unmarshal(bits)
}

// Copy creates a copy of the Node.
func (n *Node) Copy() node.Node {
	return &Node{
		Block: n.Block,
	}
}

func (n *Node) Cid() cid.Cid {
	return n.Block.Cid()
}

func (n *Node) Loggable() map[string]interface{} {
	return n.Block.Loggable()
}

func (n *Node) Links() []*node.Link {
	return nil
}

func (n *Node) Resolve(path []string) (interface{}, []string, error) {
	return nil, nil, ErrUnimplemented
}

func (n *Node) ResolveLink([]string) (*format.Link, []string, error) {
	return nil, nil, ErrUnimplemented
}

func (n *Node) Size() (uint64, error) {
	return uint64(len(n.RawData())), nil
}

func (n *Node) Stat() (*node.NodeStat, error) {
	return &node.NodeStat{}, nil
}

func (n *Node) Tree(path string, depth int) []string {
	return nil
}
