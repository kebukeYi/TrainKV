package lsm

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEmptyBuilder(t *testing.T) {
	opts := &Options{BloomFalsePositive: 0.1}
	b := newSSTBuilder(opts)
	require.Equal(t, []byte{}, b.Finish())
}
