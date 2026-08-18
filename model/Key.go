package model

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
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

// KeyTsBufPool 供提交路径复用的 key+8 缓冲池;
// 池化 *[]byte 指针: 指针装箱进 interface{} 零分配, 且缓冲可在盒内增长保留;
var KeyTsBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 64)
		return &b
	},
}

// KeyWithTsPooled 与 KeyWithTs 语义相同, 但缓冲取自 KeyTsBufPool;
// 仅限生命周期可控制的调用方使用(如事务提交路径), 用后须将返回的 *[]byte 归还池中;
func KeyWithTsPooled(key []byte, ts uint64) *[]byte {
	need := len(key) + 8
	bufPtr := KeyTsBufPool.Get().(*[]byte)
	if cap(*bufPtr) < need {
		*bufPtr = make([]byte, need)
	}
	*bufPtr = (*bufPtr)[:need]
	copy(*bufPtr, key)
	binary.BigEndian.PutUint64((*bufPtr)[len(key):], math.MaxUint64-ts)
	return bufPtr
}

func ParseTsVersion(key []byte) uint64 {
	if len(key) <= 8 {
		panic("key is too short;")
	}
	ts := binary.BigEndian.Uint64(key[len(key)-8:])
	return math.MaxUint64 - ts
}

// ParseKey 祛除掉版本信息之后的key;
func ParseKey(key []byte) []byte {
	if len(key) < 8 {
		panic("key is too short;")
	}
	return key[:len(key)-8]
}

func SameKeyNoTs(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(ParseKey(src), ParseKey(dst))
}

func SafeCopy(dst, src []byte) []byte {
	dst = make([]byte, len(src))
	copy(dst, src)
	return dst
}
