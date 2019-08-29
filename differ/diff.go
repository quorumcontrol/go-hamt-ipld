package differ

import (
	"context"

	"github.com/quorumcontrol/go-hamt-ipld"
	"github.com/quorumcontrol/go-hamt-ipld/goipldpb"
	"github.com/quorumcontrol/go-hamt-ipld/pb"
	"golang.org/x/xerrors"
)

// FindNew takes an existing HAMT and returns the new Key/Value pairs found in the newHamt
func FindNew(ctx context.Context, cs *hamt.CborIpldStore, existingHamt *hamt.Node, newHamt *hamt.Node) ([]*pb.KV, error) {
	firstWrapped, err := goipldpb.WrapObject(existingHamt)
	if err != nil {
		return nil, xerrors.Errorf("error wrapping: %w", err)
	}
	newWrapped, err := goipldpb.WrapObject(newHamt)
	if err != nil {
		return nil, xerrors.Errorf("error wrapping: %w", err)
	}
	// if the first nodes are equal, then the whole thing is equal
	// so just interrupt
	if firstWrapped.Cid().Equals(newWrapped.Cid()) {
		return nil, nil
	}

	return getDiffFromNodes(ctx, cs, existingHamt, newHamt)
}

func getDiffFromNodes(ctx context.Context, cs *hamt.CborIpldStore, existingHamt *hamt.Node, newHamt *hamt.Node) ([]*pb.KV, error) {
	newPairs := make([]*pb.KV, 0)
	for i, pointer := range newHamt.Pointers {
		if len(existingHamt.Pointers) > 0 && len(existingHamt.Pointers) < i {
			existingPointer := existingHamt.Pointers[i]
			if !pointer.Link().Defined() && pointer.Link().Equals(existingPointer.Link()) {
				continue // the links are the same, just continue
			}
		}

		//otherwise there might be something different on this path
		// for now do the dumb thing and get all the KVs and if they aren't in the existing,
		// return them as new

		var vals []*pb.KV
		if pointer.Link().Defined() {
			n, err := hamt.LoadNode(ctx, cs, pointer.Link())
			if err != nil {
				return nil, xerrors.Errorf("error loading node: %w", err)
			}
			vals, err = n.AllPairs(ctx)
			if err != nil {
				return nil, xerrors.Errorf("error getting pairs: %w", err)
			}
		} else {
			vals = pointer.Kvs
		}

		for _, kv := range vals {
			existing, err := existingHamt.GetKV(ctx, kv.Key)
			if err != nil && err != hamt.ErrNotFound {
				return nil, xerrors.Errorf("error checking existance: %w", err)
			}
			if err == hamt.ErrNotFound || !kv.Equals(existing) {
				newPairs = append(newPairs, kv)
			}
		}
	}
	return newPairs, nil
}
