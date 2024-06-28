package model

import "encoding/binary"

// ValueExt is the value with meta and expiresAt for skipList;
type ValueExt struct {
	Meta      byte // delete or normal
	Value     []byte
	ExpiresAt int64

	Version uint64 // This field is not serialized. Only for internal usage.
}

func (val *ValueExt) EncodeValSize() uint32 {
	size := len(val.Value) + 1
	enc := sizeVarint(uint64(val.ExpiresAt))
	return uint32(size + enc)
}

func (val *ValueExt) EncodeVal(buf []byte) uint32 {
	buf[0] = val.Meta
	sz := binary.PutVarint(buf[1:], val.ExpiresAt)
	n := copy(buf[sz+1:], val.Value)
	return uint32(sz + n + 1)
}

func (val *ValueExt) DecodeVal(buf []byte) {
	val.Meta = buf[0]
	var n int
	val.ExpiresAt, n = binary.Varint(buf[1:])
	val.Value = buf[n+1:]
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
