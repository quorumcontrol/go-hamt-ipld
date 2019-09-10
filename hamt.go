package hamt

import (
	"context"
	"fmt"
	"math/big"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/quorumcontrol/go-hamt-ipld/pb"

	cid "github.com/ipfs/go-cid"
	murmur3 "github.com/spaolacci/murmur3"
)

const arrayWidth = 3

type pointerSlice []*Pointer

func (ps pointerSlice) toProtoBufs() []*pb.Pointer {
	ret := make([]*pb.Pointer, len(ps))
	for i, p := range ps {
		ret[i] = p.Pointer
	}
	return ret
}

func fromProtobufs(pbs []*pb.Pointer) pointerSlice {
	ret := make(pointerSlice, len(pbs))

	for i, p := range pbs {
		ret[i] = &Pointer{Pointer: p}
	}
	return ret
}

type Node struct {
	Bitfield *big.Int     `refmt:"bf"`
	Pointers pointerSlice `refmt:"p"`

	// for fetching and storing children
	store  *CborIpldStore
	pbNode *pb.Node
}

func (n *Node) Marshal() ([]byte, error) {
	n.populatePbNode()
	return n.pbNode.Marshal()
}

func (n *Node) populatePbNode() {
	if n.pbNode == nil {
		n.pbNode = new(pb.Node)
	}
	n.pbNode.Bitfield = n.Bitfield.Bytes()
	n.pbNode.Pointers = n.Pointers.toProtoBufs()
}

func (n *Node) Unmarshal(bits []byte) error {
	pbNode := new(pb.Node)
	if err := pbNode.Unmarshal(bits); err != nil {
		return err
	}
	n.Bitfield = new(big.Int).SetBytes(pbNode.Bitfield)
	n.Pointers = fromProtobufs(pbNode.Pointers)
	return nil
}

// Reset implements the proto.Message interface
func (n *Node) Reset() {
	n.Bitfield = big.NewInt(0)
	n.Pointers = make(pointerSlice, 0)
	n.populatePbNode()
}

// String implements the proto.Message interface
func (n *Node) String() string {
	n.populatePbNode()
	return n.pbNode.String()
}

// ProtoMessage implements the proto.Message interface
func (n *Node) ProtoMessage() {}

func NewNode(cs *CborIpldStore) *Node {
	return &Node{
		pbNode:   new(pb.Node),
		Bitfield: big.NewInt(0),
		Pointers: make(pointerSlice, 0),
		store:    cs,
	}
}

type Pointer struct {
	*pb.Pointer
	// cached node to avoid too many serialization operations
	cache *Node
}

func newPointer() *Pointer {
	return &Pointer{Pointer: new(pb.Pointer)}
}

var hash = func(k string) []byte {
	h := murmur3.New128()
	h.Write([]byte(k))
	return h.Sum(nil)
}

func (n *Node) Find(ctx context.Context, k string) (interface{}, error) {
	var out interface{}
	err := n.getValue(ctx, hash(k), 0, k, func(kv *pb.KV) error {
		err := cbor.DecodeInto(kv.Value, &out)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (n *Node) GetKV(ctx context.Context, k string) (*pb.KV, error) {
	var out *pb.KV
	err := n.getValue(ctx, hash(k), 0, k, func(kv *pb.KV) error {
		out = kv
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (n *Node) Delete(ctx context.Context, k string) error {
	return n.modifyValue(ctx, hash(k), 0, k, nil)
}

var ErrNotFound = fmt.Errorf("not found")
var ErrMaxDepth = fmt.Errorf("attempted to traverse hamt beyond max depth")

func (n *Node) getValue(ctx context.Context, hv []byte, depth int, k string, cb func(*pb.KV) error) error {
	if depth >= len(hv) {
		return ErrMaxDepth
	}

	idx := hv[depth]
	if n.Bitfield.Bit(int(idx)) == 0 {
		return ErrNotFound
	}

	cindex := byte(n.indexForBitPos(int(idx)))

	c := n.getChild(cindex)
	if c.isShard() {
		chnd, err := c.loadChild(ctx, n.store)
		if err != nil {
			return err
		}

		return chnd.getValue(ctx, hv, depth+1, k, cb)
	}

	for _, kv := range c.Kvs {
		if kv.Key == k {
			return cb(kv)
		}
	}

	return ErrNotFound
}

func (p *Pointer) loadChild(ctx context.Context, ns *CborIpldStore) (*Node, error) {
	if p.cache != nil {
		return p.cache, nil
	}

	out, err := LoadNode(ctx, ns, p.Link())
	if err != nil {
		return nil, err
	}

	p.cache = out
	return out, nil
}

func LoadNode(ctx context.Context, cs *CborIpldStore, c cid.Cid) (*Node, error) {
	var out Node
	if err := cs.Get(ctx, c, &out); err != nil {
		return nil, err
	}

	out.store = cs
	return &out, nil
}

func (n *Node) checkSize(ctx context.Context) (uint64, error) {
	c, err := n.store.Put(ctx, n)
	if err != nil {
		return 0, err
	}

	blk, err := n.store.Nodes.Get(ctx, c)
	if err != nil {
		return 0, err
	}

	totsize := uint64(len(blk.RawData()))
	for _, ch := range n.Pointers {
		if ch.isShard() {
			chnd, err := ch.loadChild(ctx, n.store)
			if err != nil {
				return 0, err
			}
			chsize, err := chnd.checkSize(ctx)
			if err != nil {
				return 0, err
			}
			totsize += chsize
		}
	}

	return totsize, nil
}

func (n *Node) AllPairs(ctx context.Context) ([]*pb.KV, error) {
	vals := make([]*pb.KV, 0)
	for _, ch := range n.Pointers {
		if ch.isShard() {
			chnd, err := ch.loadChild(ctx, n.store)
			if err != nil {
				return nil, err
			}
			newVals, err := chnd.AllPairs(ctx)
			if err != nil {
				return nil, err
			}
			vals = append(vals, newVals...)
		} else {
			vals = append(vals, ch.Kvs...)
		}
	}
	return vals, nil
}

func (n *Node) Flush(ctx context.Context) error {
	for _, p := range n.Pointers {
		if p.cache != nil {
			if err := p.cache.Flush(ctx); err != nil {
				return err
			}

			c, err := n.store.Put(ctx, p.cache)
			if err != nil {
				return err
			}

			p.cache = nil
			// if p is a shard no need to keep the Kvs around
			p.Kvs = nil
			p.SetLink(c)
		}
	}

	return nil
}

func (n *Node) Set(ctx context.Context, k string, v interface{}) error {
	nd, err := WrapObject(v)
	if err != nil {
		return err
	}
	err = n.modifyValue(ctx, hash(k), 0, k, nd.RawData())
	return err
}

func (n *Node) cleanChild(chnd *Node, cindex byte) error {
	l := len(chnd.Pointers)
	switch {
	case l == 0:
		return fmt.Errorf("incorrectly formed HAMT")
	case l == 1:
		// TODO: only do this if its a value, cant do this for shards unless pairs requirements are met.

		ps := chnd.Pointers[0]
		if ps.isShard() {
			return nil
		}

		return n.setChild(cindex, ps)
	case l <= arrayWidth:
		var chvals []*pb.KV
		for _, p := range chnd.Pointers {
			if p.isShard() {
				return nil
			}

			for _, sp := range p.Kvs {
				if len(chvals) == arrayWidth {
					return nil
				}
				chvals = append(chvals, sp)
			}
		}
		return n.setChild(cindex, &Pointer{Pointer: &pb.Pointer{Kvs: chvals}})
	default:
		return nil
	}
}

func (n *Node) modifyValue(ctx context.Context, hv []byte, depth int, k string, v []byte) error {
	if depth >= len(hv) {
		return ErrMaxDepth
	}
	idx := int(hv[depth])

	if n.Bitfield.Bit(idx) != 1 {
		err := n.insertChild(idx, k, v)
		return err
	}

	cindex := byte(n.indexForBitPos(idx))

	child := n.getChild(cindex)
	if child.isShard() {
		chnd, err := child.loadChild(ctx, n.store)
		if err != nil {
			return err
		}

		if err := chnd.modifyValue(ctx, hv, depth+1, k, v); err != nil {
			return err
		}

		// CHAMP optimization, ensure trees look correct after deletions
		if v == nil {
			if err := n.cleanChild(chnd, cindex); err != nil {
				return err
			}
		}

		return nil
	}

	if v == nil {
		for i, p := range child.Kvs {
			if p.Key == k {
				if len(child.Kvs) == 1 {
					return n.rmChild(cindex, idx)
				}

				copy(child.Kvs[i:], child.Kvs[i+1:])
				child.Kvs = child.Kvs[:len(child.Kvs)-1]
				return nil
			}
		}
		return ErrNotFound
	}

	// check if key already exists
	for _, p := range child.Kvs {
		if p.Key == k {
			p.Value = v
			return nil
		}
	}

	// If the array is full, create a subshard and insert everything into it
	if len(child.Kvs) >= arrayWidth {
		sub := NewNode(n.store)
		if err := sub.modifyValue(ctx, hv, depth+1, k, v); err != nil {
			return err
		}

		for _, p := range child.Kvs {
			if err := sub.modifyValue(ctx, hash(p.Key), depth+1, p.Key, p.Value); err != nil {
				return err
			}
		}

		c, err := n.store.Put(ctx, sub)
		if err != nil {
			return err
		}

		p := new(pb.Pointer)
		p.SetLink(c)
		return n.setChild(cindex, &Pointer{Pointer: p})
	}

	// otherwise insert the new element into the array in order
	np := &pb.KV{Key: k, Value: v}
	for i := 0; i < len(child.Kvs); i++ {
		if k < child.Kvs[i].Key {
			child.Kvs = append(child.Kvs[:i], append([]*pb.KV{np}, child.Kvs[i:]...)...)
			return nil
		}
	}
	child.Kvs = append(child.Kvs, np)
	return nil
}

func (n *Node) insertChild(idx int, k string, v []byte) error {
	if v == nil {
		return ErrNotFound
	}

	i := n.indexForBitPos(idx)
	n.Bitfield.SetBit(n.Bitfield, idx, 1)

	p := &Pointer{Pointer: &pb.Pointer{Kvs: []*pb.KV{{Key: k, Value: v}}}}
	n.Pointers = append(n.Pointers[:i], append([]*Pointer{p}, n.Pointers[i:]...)...)
	return nil
}

func (n *Node) setChild(i byte, p *Pointer) error {
	n.Pointers[i] = p
	return nil
}

func (n *Node) rmChild(i byte, idx int) error {
	copy(n.Pointers[i:], n.Pointers[i+1:])
	n.Pointers = n.Pointers[:len(n.Pointers)-1]
	n.Bitfield.SetBit(n.Bitfield, idx, 0)

	return nil
}

func (n *Node) getChild(i byte) *Pointer {
	if int(i) >= len(n.Pointers) || i < 0 {
		return nil
	}

	return n.Pointers[i]
}

func (n *Node) Copy() *Node {
	nn := NewNode(n.store)
	nn.Bitfield.Set(n.Bitfield)
	nn.Pointers = make([]*Pointer, len(n.Pointers))

	for i, p := range n.Pointers {
		pp := &Pointer{Pointer: new(pb.Pointer)}
		if p.cache != nil {
			pp.cache = p.cache.Copy()
		}
		pp.SetLink(p.Link())
		if p.Kvs != nil {
			pp.Kvs = make([]*pb.KV, len(p.Kvs))
			for j, kv := range p.Kvs {
				pp.Kvs[j] = &pb.KV{Key: kv.Key, Value: kv.Value}
			}
		}
		nn.Pointers[i] = pp
	}

	return nn
}

func (p *Pointer) isShard() bool {
	return p.Link().Defined()
}
