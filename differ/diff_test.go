package differ

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"

	"github.com/quorumcontrol/go-hamt-ipld"
	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	ctx := context.Background()
	vals := make(map[string][]byte)
	var keys []string
	for i := 0; i < 100000; i++ {
		s := randString()
		vals[s] = randValue()
		keys = append(keys, s)
	}

	cs := hamt.NewCborStore()
	hamt1 := hamt.NewNode(cs)
	for _, k := range keys {
		hamt1.Set(ctx, k, vals[k])
	}
	hamt2 := hamt.NewNode(cs)
	for _, k := range keys {
		hamt2.Set(ctx, k, vals[k])
	}
	hamt1.Flush(ctx)
	hamt2.Flush(ctx)

	diff, err := FindNew(ctx, cs, hamt1, hamt2)
	require.Nil(t, err)
	require.Len(t, diff, 0)

	err = hamt1.Delete(ctx, keys[0])
	require.Nil(t, err)
	hamt1.Flush(ctx)

	diff, err = FindNew(ctx, cs, hamt1, hamt2)
	require.Nil(t, err)
	require.Len(t, diff, 1)

	for i := 1; i < 1001; i++ {
		hamt1.Delete(ctx, keys[i])
	}
	hamt1.Flush(ctx)
	diff, err = FindNew(ctx, cs, hamt1, hamt2)
	require.Nil(t, err)
	require.Len(t, diff, 1001)
}

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
