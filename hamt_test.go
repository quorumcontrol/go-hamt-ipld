package hamt

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/quorumcontrol/go-hamt-ipld/goipldpb"
)

func randString() string {
	buf := make([]byte, 18)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

func randValue() []byte {
	buf := make([]byte, 30)
	rand.Read(buf)
	return buf
}

func dotGraph(n *Node) {
	fmt.Println("digraph foo {")
	name := 0
	dotGraphRec(n, &name)
	fmt.Println("}")
}

var identityHash = func(k string) []byte {
	res := make([]byte, 32)
	copy(res, []byte(k))
	return res
}

var shortIdentityHash = func(k string) []byte {
	res := make([]byte, 16)
	copy(res, []byte(k))
	return res
}

var murmurHash = hash

func TestCanonicalStructure(t *testing.T) {
	hash = identityHash
	defer func() {
		hash = murmurHash
	}()
	addAndRemoveKeys(t, []string{"K"}, []string{"B"})
	addAndRemoveKeys(t, []string{"K0", "K1", "KAA1", "KAA2", "KAA3"}, []string{"KAA4"})
}

func TestOverflow(t *testing.T) {
	hash = identityHash
	defer func() {
		hash = murmurHash
	}()
	keys := make([]string, 4)
	for i := range keys {
		keys[i] = strings.Repeat("A", 32) + fmt.Sprintf("%d", i)
	}

	cs := NewCborStore()
	n := NewNode(cs)
	for _, k := range keys[:3] {
		if err := n.Set(context.Background(), k, "foobar"); err != nil {
			t.Error(err)
		}
	}

	// Try forcing the depth beyond 32
	if err := n.Set(context.Background(), keys[3], "bad"); err != ErrMaxDepth {
		t.Errorf("expected error %q, got %q", ErrMaxDepth, err)
	}

	// Force _to_ max depth.
	if err := n.Set(context.Background(), keys[3][1:], "bad"); err != nil {
		t.Error(err)
	}

	// Now, try fetching with a shorter hash function.
	hash = shortIdentityHash
	_, err := n.Find(context.Background(), keys[0])
	if err != ErrMaxDepth {
		t.Errorf("expected error %q, got %q", ErrMaxDepth, err)
	}
}

func addAndRemoveKeys(t *testing.T, keys []string, extraKeys []string) {
	ctx := context.Background()
	vals := make(map[string][]byte)
	for i := 0; i < len(keys); i++ {
		s := keys[i]
		vals[s] = randValue()
	}

	cs := NewCborStore()
	begn := NewNode(cs)

	for _, k := range keys {
		fmt.Println("set ", k)
		if err := begn.Set(ctx, k, vals[k]); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("start flush")
	bef := time.Now()
	if err := begn.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("flush took: ", time.Since(bef))
	c, err := cs.Put(ctx, begn)
	if err != nil {
		t.Fatal(err)
	}

	var n Node
	if err := cs.Get(ctx, c, &n); err != nil {
		t.Fatal(err)
	}
	n.store = cs

	for k, v := range vals {
		out, err := n.Find(ctx, k)
		if err != nil {
			t.Fatal("should have found the thing")
		}
		if !bytes.Equal(out.([]byte), v) {
			t.Fatal("got wrong value after value change")
		}
	}

	// create second hamt by adding and deleting the extra keys
	for i := 0; i < len(extraKeys); i++ {
		begn.Set(ctx, extraKeys[i], randValue())
	}
	for i := 0; i < len(extraKeys); i++ {
		if err := begn.Delete(ctx, extraKeys[i]); err != nil {
			t.Fatal(err)
		}
	}

	if err := begn.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	c2, err := cs.Put(ctx, begn)
	if err != nil {
		t.Fatal(err)
	}

	var n2 Node
	if err := cs.Get(ctx, c2, &n2); err != nil {
		t.Fatal(err)
	}
	n2.store = cs
	if !nodesEqual(t, cs, &n, &n2) {
		t.Fatal("nodes should be equal")
	}
}

func dotGraphRec(n *Node, name *int) {
	cur := *name
	for _, p := range n.Pointers {
		if p.isShard() {
			*name++
			fmt.Printf("\tn%d -> n%d;\n", cur, *name)
			nd, err := p.loadChild(context.Background(), n.store)
			if err != nil {
				panic(err)
			}

			dotGraphRec(nd, name)
		} else {
			var names []string
			for _, pt := range p.Kvs {
				names = append(names, pt.Key)
			}
			fmt.Printf("\tn%d -> n%s;\n", cur, strings.Join(names, "_"))
		}
	}
}

type hamtStats struct {
	totalNodes int
	totalKvs   int
	counts     map[int]int
}

func stats(n *Node) *hamtStats {
	st := &hamtStats{counts: make(map[int]int)}
	statsrec(n, st)
	return st
}

func statsrec(n *Node, st *hamtStats) {
	st.totalNodes++
	for _, p := range n.Pointers {
		if p.isShard() {
			nd, err := p.loadChild(context.Background(), n.store)
			if err != nil {
				panic(err)
			}

			statsrec(nd, st)
		} else {
			st.totalKvs += len(p.Kvs)
			st.counts[len(p.Kvs)]++
		}
	}
}

func TestHash(t *testing.T) {
	h1 := hash("abcd")
	h2 := hash("abce")
	if h1[0] == h2[0] && h1[1] == h2[1] && h1[3] == h2[3] {
		t.Fatal("Hash should give different strings different hash prefixes")
	}
}

func TestSetGet(t *testing.T) {
	ctx := context.Background()
	vals := make(map[string][]byte)
	var keys []string
	for i := 0; i < 100000; i++ {
		s := randString()
		vals[s] = randValue()
		keys = append(keys, s)
	}

	cs := NewCborStore()
	begn := NewNode(cs)
	for _, k := range keys {
		begn.Set(ctx, k, vals[k])
	}

	size, err := begn.checkSize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mapsize := 0
	for k, v := range vals {
		mapsize += (len(k) + len(v))
	}
	fmt.Printf("Total size is: %d, size of keys+vals: %d, overhead: %.2f\n", size, mapsize, float64(size)/float64(mapsize))
	fmt.Println(stats(begn))

	fmt.Println("start flush")
	bef := time.Now()
	if err := begn.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("flush took: ", time.Since(bef))

	fmt.Println("put of root node start")
	bef = time.Now()
	c, err := cs.Put(ctx, begn)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("put of root node took: ", time.Since(bef), " pointer len: ", len(begn.Pointers))

	var n Node
	if err := cs.Get(ctx, c, &n); err != nil {
		t.Fatal(err)
	}
	n.store = cs

	bef = time.Now()
	for k, v := range vals {
		out, err := n.Find(ctx, k)
		if err != nil {
			t.Fatal("should have found the thing: ", err)
		}
		if !bytes.Equal(out.([]byte), v) {
			t.Fatal("got wrong value")
		}
	}
	fmt.Println("finds took: ", time.Since(bef))

	for i := 0; i < 100; i++ {
		_, err := n.Find(ctx, randString())
		if err != ErrNotFound {
			t.Fatal("should have gotten ErrNotFound, instead got: ", err)
		}
	}

	for k := range vals {
		next := randValue()
		n.Set(ctx, k, next)
		vals[k] = next
	}

	for k, v := range vals {
		out, err := n.Find(ctx, k)
		if err != nil {
			t.Fatal("should have found the thing")
		}
		if !bytes.Equal(out.([]byte), v) {
			t.Fatal("got wrong value after value change")
		}
	}

	for i := 0; i < 100; i++ {
		err := n.Delete(ctx, randString())
		if err != ErrNotFound {
			t.Fatal("should have gotten ErrNotFound, instead got: ", err)
		}
	}

	for _, k := range keys {
		if err := n.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
		if _, err := n.Find(ctx, k); err != ErrNotFound {
			t.Fatal("Expected ErrNotFound, got: ", err)
		}
	}
}

func nodesEqual(t *testing.T, store *CborIpldStore, n1, n2 *Node) bool {
	ctx := context.Background()
	err := n1.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n1Cid, err := store.Put(ctx, n1)
	if err != nil {
		t.Fatal(err)
	}
	err = n2.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n2Cid, err := store.Put(ctx, n2)
	if err != nil {
		t.Fatal(err)
	}
	return n1Cid.Equals(n2Cid)
}

func TestCopy(t *testing.T) {
	ctx := context.Background()
	cs := NewCborStore()

	n := NewNode(cs)
	nc := n.Copy()
	if !nodesEqual(t, cs, n, nc) {
		t.Fatal("nodes should be equal")
	}
	n.Set(ctx, "key", []byte{0x01})
	if nodesEqual(t, cs, n, nc) {
		t.Fatal("nodes should not be equal -- we set a key on n")
	}
	nc = n.Copy()
	nc.Set(ctx, "key2", []byte{0x02})
	if nodesEqual(t, cs, n, nc) {
		t.Fatal("nodes should not be equal -- we set a key on nc")
	}
	n = nc.Copy()
	if !nodesEqual(t, cs, n, nc) {
		t.Fatal("nodes should be equal")
	}
}

func TestCopyCopiesNilSlices(t *testing.T) {
	cs := NewCborStore()

	n := NewNode(cs)
	pointer := newPointer()
	n.Pointers = append(n.Pointers, pointer)

	if n.Pointers[0].Kvs != nil {
		t.Fatal("Expected uninitialize slice to be nil")
	}

	nc := n.Copy()

	if nc.Pointers[0].Kvs != nil {
		t.Fatal("Expected copied nil slices to be nil")
	}
}

func TestCopyWithoutFlush(t *testing.T) {
	ctx := context.Background()
	cs := NewCborStore()

	count := 200
	n := NewNode(cs)
	for i := 0; i < count; i++ {
		n.Set(ctx, fmt.Sprintf("key%d", i), []byte{byte(i)})
	}

	n.Flush(ctx)

	for i := 0; i < count; i++ {
		n.Set(ctx, fmt.Sprintf("key%d", i), []byte{byte(count + i)})
	}

	nc := n.Copy()

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key%d", i)

		val, err := n.Find(ctx, key)
		if err != nil {
			t.Fatalf("should have found key %s in original", key)
		}

		valCopy, err := nc.Find(ctx, key)
		if err != nil {
			t.Fatalf("should have found key %s in copy", key)
		}

		if val.([]byte)[0] != valCopy.([]byte)[0] {
			t.Fatalf("copy does not equal original (%d != %d)", valCopy.([]byte)[0], val.([]byte)[0])
		}
	}
}

func TestValueLinking(t *testing.T) {
	ctx := context.Background()
	cs := NewCborStore()

	thingy1 := map[string]string{"cat": "dog"}
	c1, err := cs.Put(ctx, thingy1)
	if err != nil {
		t.Fatal(err)
	}

	thingy2 := map[string]interface{}{
		"one": c1,
		"foo": "bar",
	}

	n := NewNode(cs)

	if err := n.Set(ctx, "cat", thingy2); err != nil {
		t.Fatal(err)
	}

	tcid, err := cs.Put(ctx, n)
	if err != nil {
		t.Fatal(err)
	}

	blk, err := cs.Nodes.Get(ctx, tcid)
	if err != nil {
		t.Fatal(err)
	}

	var decodedNode Node
	goipldpb.DecodeInto(blk.RawData(), &decodedNode)

	var decodedVal map[string]interface{}
	cbor.DecodeInto(decodedNode.Pointers[0].Kvs[0].Value, &decodedVal)

	fmt.Println("thingy1", c1)
	fmt.Println(decodedVal)
}
