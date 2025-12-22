package model

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"
)

// CompareKeyWithTs MergingIterator.Less()使用;
func CompareKeyWithTs(key1, key2 []byte) int {
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	key1Version := key1[len(key1)-8:]
	key2Version := key2[len(key2)-8:]
	return bytes.Compare(key1Version, key2Version)
}

func KeyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}

func ParseTsVersion(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	ts := binary.BigEndian.Uint64(key[len(key)-8:])
	return math.MaxUint64 - ts
}

// ParseKey 祛除掉版本信息之后的key;
func ParseKey(key []byte) []byte {
	if len(key) < 8 {
		return key
	}
	return key[:len(key)-8]
}

func SameKeyNoTs(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(ParseKey(src), ParseKey(dst))
}

func KeyWithTestTs(key []byte, version uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], version)
	return out
}

func ParseTestTsVersion(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	ts := binary.BigEndian.Uint64(key[len(key)-8:])
	return ts
}

func SafeCopy(dst, src []byte) []byte {
	dst = make([]byte, len(src))
	copy(dst, src)
	return dst
}

func NewCurVersion() uint64 {
	return uint64(time.Now().UnixNano())
}
