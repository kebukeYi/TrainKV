package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"time"
)

// CompareKeyNoTs skipList.key()  table.biggestKey()  block.baseKey()
func CompareKeyNoTs(key1, key2 []byte) int {
	common.CondPanic(len(key1) <= 8 || len(key2) <= 8, fmt.Errorf("%s,%s < 8", string(key1), string(key2)))
	return bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8])
}

func CompareKeyWithTs(key1, key2 []byte) int {
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	key1Version := key1[len(key1)-8:]
	key2Version := key2[len(key2)-8:]
	return bytes.Compare(key1Version, key2Version)
}

func ParseTsVersion(key []byte) int64 {
	if len(key) <= 8 {
		return 0
	}
	timestamp := binary.BigEndian.Uint64(key[len(key)-8:])
	return int64(timestamp)
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

func KeyWithTs(key []byte) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], NewCurVersion())
	return out
}

func KeyWithTestTs(key []byte, version uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], version)
	return out
}

func SafeCopy(des, src []byte) []byte {
	des = make([]byte, len(src))
	copy(des, src)
	return des
}

func NewCurVersion() uint64 {
	// return uint64(time.Now().UnixNano() / 1e9)
	return uint64(time.Now().UnixNano())
}
