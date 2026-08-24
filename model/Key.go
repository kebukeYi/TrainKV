package model

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
)

// CompareKeyWithTs MergingIterator.Less()使用;
// 先比 raw key (len-8) 再比 ts: raw key 不同则长度也可能不同 (前缀关系), 必须按 raw key 区比较;
// 长度相同时 (同 raw key 或同长 raw key) 直接全量比较即可 —— 首个不同字节要么落在 raw 区, 要么落在 ts 区, 语义一致;
func CompareKeyWithTs(key1, key2 []byte) int {
	n1, n2 := len(key1)-8, len(key2)-8
	if n1 == n2 {
		return bytes.Compare(key1, key2)
	}
	// raw key 长度不同: 比较公共前缀, 前缀短者小 (与 ts 无关);
	n := n1
	if n2 < n {
		n = n2
	}
	if cmp := bytes.Compare(key1[:n], key2[:n]); cmp != 0 {
		return cmp
	}
	if n1 < n2 {
		return -1
	}
	return 1
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
