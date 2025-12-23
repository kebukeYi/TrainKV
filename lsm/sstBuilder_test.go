package lsm

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEmptyBuilder(t *testing.T) {
	opts := &Options{BloomFalsePositive: 0.1}
	b := NewSSTBuilder(opts)
	//b.Add(&model.Entry{}, false)
	require.Equal(t, []byte{}, b.Finish())
}

type testEntry struct {
	key []byte
	val []byte
}

func TestBaseKey(t *testing.T) {
	entry := &testEntry{}
	if len(entry.key) == 0 {
		entry.key = append(entry.key, 4, 5)
	}
	fmt.Println("slice1:", entry.key) // 输出: slice1: [1 2 3 4 5]

	if len(entry.val) == 0 {
		entry.val = append(entry.val[:0], 4, 5)
	}
	fmt.Println("slice2:", entry.val) // 输出: slice2: [4 5]
}
