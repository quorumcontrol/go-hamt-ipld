package goipldpb

import (
	"math"
	"testing"

	"github.com/quorumcontrol/go-hamt-ipld/pb"
)

func TestWrapObject(t *testing.T) {
	kv := &pb.KV{
		Key:   "hi",
		Value: []byte("hi"),
	}
	_, err := WrapObject(kv, uint64(math.MaxUint64), -1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDecodeInto(t *testing.T) {
	kv := &pb.KV{
		Key:   "hi",
		Value: []byte("hi"),
	}
	nd, err := WrapObject(kv, uint64(math.MaxUint64), -1)
	if err != nil {
		t.Fatal(err)
	}
	retKv := new(pb.KV)
	err = DecodeInto(nd.RawData(), retKv)
	if err != nil {
		t.Fatal(err)
	}
	if retKv.Key != kv.Key {
		t.Fatalf("retKv key %s did not match %s", retKv.Key, kv.Key)
	}
}
