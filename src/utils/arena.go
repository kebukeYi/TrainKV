package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

const (
	// 8 - 1  = 7 字节
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
	//MaxNodeSize = int(unsafe.Sizeof(&skipNode{}))
	MaxNodeSize = int(unsafe.Sizeof(skipNode{}))
	offsetSize  = int(unsafe.Sizeof(uint32(0)))
)

type Arena struct {
	data       []byte
	sizes      uint32
	shouldGrow bool
}

func NewArena(n int64) *Arena {
	return &Arena{
		data:  make([]byte, n),
		sizes: 1,
	}
}

func (a *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&a.sizes, sz)
	if !a.shouldGrow {
		AssertTrue(int(offset) <= len(a.data))
		return offset - sz
	}
	if int(offset) > len(a.data)-MaxNodeSize {
		growBy := uint32(len(a.data))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newData := make([]byte, len(a.data)+int(growBy))
		copy(newData, a.data)
		a.data = newData
	}
	return offset - sz
}

func (a *Arena) size() int64 {
	return int64(atomic.LoadUint32(&a.sizes))
}

func (a *Arena) AllocateNode(height int) uint32 {
	unUsedSize := (maxHeight - height) * offsetSize
	u := uint32(MaxNodeSize - unUsedSize + nodeAlign)
	offset := a.allocate(u)
	offset = (offset + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return offset
}

func (a *Arena) getNode(offset uint32) *skipNode {
	if offset == 0 {
		return nil
	}
	return (*skipNode)(unsafe.Pointer(&a.data[offset]))
}

func (a *Arena) getNodeOffset(node *skipNode) uint32 {
	if node == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&a.data[0])))
}

func (a *Arena) PutKey(key []byte) uint32 {
	keyLen := uint32(len(key))
	offset := a.allocate(keyLen)
	buf := a.data[offset : offset+keyLen]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (a *Arena) PutVal(val []byte) uint32 {
	keyLen := uint32(len(val))
	offset := a.allocate(keyLen)
	bytes := a.data[offset : offset+keyLen]
	copy(bytes, val)
	return offset
}

func (a *Arena) getKey(offset uint32, size uint32) []byte {
	return a.data[offset : offset+size]
}

func (a *Arena) getVal(offset uint32, size uint32) []byte {
	return a.data[offset : offset+size]
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
