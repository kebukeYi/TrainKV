package skl

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/utils"
	"sync/atomic"
	"unsafe"
)

const (
	// 8 - 1  = 7 字节
	nodeAlign  = int(unsafe.Sizeof(uint64(0))) - 1
	offsetSize = int(unsafe.Sizeof(uint32(0)))
)

type Arena struct {
	data       []byte
	sizes      uint32
	shouldGrow bool
}

func NewArena(n int64) *Arena {
	return &Arena{
		data:       make([]byte, n),
		sizes:      1,
		shouldGrow: true,
	}
}

func (a *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&a.sizes, sz)
	if !a.shouldGrow {
		fmt.Printf("Arena size: %d, len(d.data): %d ,grow: %v; \n", a.sizes, len(a.data), a.shouldGrow)
		utils.AssertTrue(int(offset) <= len(a.data))
		return offset - sz
	}
	if int(offset) > len(a.data)-MaxSkipNodeSize {
		growBy := uint32(len(a.data))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newData := make([]byte, len(a.data)+int(growBy))
		utils.AssertTrue(len(a.data) == copy(newData, a.data))
		a.data = newData
	}
	return offset - sz
}

func (a *Arena) size() int64 {
	return int64(atomic.LoadUint32(&a.sizes))
}

func (a *Arena) AllocateNode(height int) uint32 {
	unUsedSize := (maxHeight - height) * offsetSize
	u := uint32(MaxSkipNodeSize - unUsedSize + nodeAlign)
	n := a.allocate(u)
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
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
	utils.AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (a *Arena) PutVal(val model.ValueExt) uint32 {
	encodeValSize := val.EncodeValSize()
	offset := a.allocate(encodeValSize)
	val.EncodeVal(a.data[offset:])
	return offset
}

func (a *Arena) getKey(offset uint32, size uint32) []byte {
	return a.data[offset : offset+size]
}

func (a *Arena) getVal(offset uint32, size uint32) (ret model.ValueExt) {
	ret.DecodeVal(a.data[offset : offset+size])
	return
}

// ChunkedArena 分块分配器: 块一经分配不再移动/释放, 已返回的切片永久有效;
// 用于迭代器 Item 的稳定副本, 把每 key 多次小分配摊销成少量块分配;
type ChunkedArena struct {
	chunks  [][]byte
	off     int
	chunkSz int
}

func NewChunkedArena(chunkSz int) *ChunkedArena {
	if chunkSz <= 0 {
		chunkSz = 64 << 10
	}
	return &ChunkedArena{chunkSz: chunkSz}
}

// Alloc 返回 n 字节的稳定切片 (0 长度返回 nil);
func (a *ChunkedArena) Alloc(n int) []byte {
	if n == 0 {
		return nil
	}
	if len(a.chunks) == 0 || a.off+n > len(a.chunks[len(a.chunks)-1]) {
		sz := a.chunkSz
		if sz < n {
			sz = n
		}
		a.chunks = append(a.chunks, make([]byte, sz))
		a.off = 0
	}
	b := a.chunks[len(a.chunks)-1][a.off : a.off+n]
	a.off += n
	return b
}
