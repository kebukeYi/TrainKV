package model

import (
	"encoding/binary"
	"github.com/kebukeYi/TrainKV/common"
	"reflect"
	"time"
	"unsafe"
)

const vptrSize = unsafe.Sizeof(ValuePtr{})

// ValuePtr 存储的是 value在vlog组件的信息;
type ValuePtr struct {
	Len    uint32
	Offset uint32
	Fid    uint32
}

func (p ValuePtr) Encode() []byte {
	buf := make([]byte, vptrSize)
	v := (*ValuePtr)(unsafe.Pointer(&buf[0]))
	*v = p
	return buf
}

func (p *ValuePtr) Decode(b []byte) {
	if uintptr(len(b)) < vptrSize {
		panic("input slice is too short")
	}
	// Directly copy the bytes using unsafe.Pointer.
	// This operation assumes that the underlying type of ValuePtr is compatible with the byte slice.
	// The cast is safe as long as the size and alignment requirements are met.
	//*(*[vptrSize]byte)(unsafe.Pointer(p)) = *(*[vptrSize]byte)(unsafe.Pointer(&b[0]))
	copy((*[vptrSize]byte)(unsafe.Pointer(p))[:], b[:vptrSize])
}

func IsValPtr(entry *Entry) bool {
	return entry.Meta&common.BitValuePointer > 0
}

func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

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

func IsDiscardEntry(vlogEntry, lsmEntry *Entry) bool {
	vlogTs := ParseTsVersion(vlogEntry.Key)
	lsmTs := lsmEntry.Version // 在跳表查询过程中,进行了 version字段 赋值;
	if vlogTs != lsmTs {
		// Version not found. Discard.
		return true
	}
	if IsDeletedOrExpired(lsmEntry.Meta, lsmEntry.ExpiresAt) {
		return true
	}
	if lsmEntry.Value == nil || len(lsmEntry.Value) == 0 {
		return true
	}
	// 是否 kv 分离数据
	if (lsmEntry.Meta & common.BitValuePointer) == 0 {
		// Key also stores the value in LSM. Discard.
		// 说明 value 已经变成了 可存储在 lsm 中了, 那就需要将 vlog 中的数据进行删除;
		return true
	}
	// 没有过期 || 还是 kv 分离类型数据
	return false
}
