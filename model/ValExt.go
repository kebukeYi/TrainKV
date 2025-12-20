package model

import "encoding/binary"

// ValueExt 把有关value的扩展信息,再封装一次; 使其方便一次性编码在 arena 空间中, 返回len和offset;
type ValueExt struct {
	Meta      byte // delete:1 or normal:0  or BitValuePointer:2
	Value     []byte
	ExpiresAt uint64

	Version uint64 // 注意: 此字段实际上并没有存储到磁盘中;
}

func (val *ValueExt) EncodeValSize() uint32 {
	size := len(val.Value) + 1 // 1B meta
	enc := sizeVarint(val.ExpiresAt)
	return uint32(size + enc)
}

func (val *ValueExt) EncodeVal(buf []byte) uint32 {
	buf[0] = val.Meta
	sz := binary.PutUvarint(buf[1:], val.ExpiresAt)
	n := copy(buf[sz+1:], val.Value)
	return uint32(sz + n + 1)
}

func (val *ValueExt) DecodeVal(buf []byte) {
	val.Meta = buf[0]
	var n int
	val.ExpiresAt, n = binary.Uvarint(buf[1:])
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
