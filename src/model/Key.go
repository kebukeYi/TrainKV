package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
	errors "trainKv/common"
)

//func CompareKey(key1, key2 []byte) int {
//	errors.CondPanic(len(key1) <= 8 || len(key2) <= 8,
//		fmt.Errorf("CompareKeys:key1:%s len(key1):%d < 8 || key2:%s len(key2):%d < 8", string(key1), len(key1), string(key2), len(key2)))
//	if compare := bytes.Compare(key1[0:len(key1)-8], key2[0:len(key2)-8]); compare != 0 {
//		return compare
//	}
//	// key 相同的情况下, 比较后面的 时间戳; 但是会返回 cmp == 0 吗? 时间戳会相同吗? 不会
//	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
//}

//func CompareBaseKeyNoTs(blockBaseKey, userKey []byte) int {
//	return bytes.Compare(blockBaseKey[:], userKey[:len(userKey)-8])
//}

// CompareKeyNoTs skipList.key()  table.biggestKey()  block.baseKey()
func CompareKeyNoTs(key1, key2 []byte) int {
	errors.CondPanic(len(key1) <= 8 || len(key2) <= 8, fmt.Errorf("%s,%s < 8", string(key1), string(key2)))
	return bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8])
}

func ParseTsVersion(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	timestamp := binary.BigEndian.Uint64(key[len(key)-8:])
	return timestamp
}

// ParseKey parses the actual key from the key bytes.
func ParseKey(key []byte) []byte {
	if len(key) < 8 {
		return key
	}
	return key[:len(key)-8]
}

func SameKey(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(ParseKey(src), ParseKey(dst))
}

func KeyWithTs(key []byte) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], NewCurVersion())
	return out
}

func SafeCopy(des, src []byte) []byte {
	return append(des[:0], src...)
}

func NewCurVersion() uint64 {
	// return uint64(time.Now().UnixNano() / 1e9)
	return uint64(time.Now().UnixNano())
}
