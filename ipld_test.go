package hamt

import (
	"context"
	"testing"

	"github.com/quorumcontrol/go-hamt-ipld/pb"
)

func TestRoundtrip(t *testing.T) {
	ctx := context.Background()

	cs := NewCborStore()
	n := NewNode(cs)
	n.Bitfield.SetBit(n.Bitfield, 5, 1)
	n.Bitfield.SetBit(n.Bitfield, 7, 1)
	n.Bitfield.SetBit(n.Bitfield, 18, 1)

	n.Pointers = []*Pointer{{Pointer: &pb.Pointer{Kvs: []*pb.KV{{Key: "foo", Value: []byte("bar")}}}}}

	c, err := cs.Put(ctx, n)
	if err != nil {
		t.Fatal(err)
	}

	var nnode Node
	if err := cs.Get(ctx, c, &nnode); err != nil {
		t.Fatal(err)
	}

	c2, err := cs.Put(ctx, &nnode)
	if err != nil {
		t.Fatal(err)
	}

	if !c.Equals(c2) {
		t.Fatal("cid mismatch")
	}
}
