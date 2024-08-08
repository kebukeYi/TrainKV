package model

import (
	"encoding/binary"
	"reflect"
	"time"
	"trainKv/common"
	"unsafe"
)

const vptrSize = unsafe.Sizeof(ValuePtr{})

type ValuePtr struct {
	Len    uint32
	Offset uint32
	Fid    uint32
}

func (p ValuePtr) Less(o *ValuePtr) bool {
	if o == nil {
		return false
	}
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}
	return p.Len < o.Len
}

func (p ValuePtr) IsZero() bool {
	return p.Fid == 0 && p.Offset == 0 && p.Len == 0
}

func (p ValuePtr) Encode() []byte {
	buf := make([]byte, vptrSize)
	v := (*ValuePtr)(unsafe.Pointer(&buf[0]))
	*v = p
	return buf
}

func (p ValuePtr) Decode(b []byte) {
	copy((*[vptrSize]byte)(unsafe.Pointer(&p))[:], b)
}

func IsValPtr(entry *Entry) bool {
	return entry.Meta&common.BitValuePointer > 0
}

// BytesToU32 converts the given byte slice to uint32
func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// U32ToBytes converts the given Uint32 to bytes
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

// BytesToU64 _
func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// U64ToBytes converts the given Uint64 to bytes
func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

func RunCallback(back func()) {
	if back != nil {
		back()
	}
}

func IsDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&common.BitDelete > 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

func IsDiscardEntry(vs *Entry) bool {
	if IsDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return true
	}
	if vs.Value == nil || len(vs.Value) == 0 {
		return true
	}
	// 是否 kv 分离数据
	if (vs.Meta & common.BitValuePointer) == 0 {
		// Key also stores the value in LSM. Discard.
		// 说明 value 已经变成了 可存储在 lsm 中了, 那就需要将 vlog 中的数据进行删除;
		return true
	}
	// 没有过期 || 还是 kv 分离类型数据
	return false
}
