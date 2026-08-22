package skl

import (
	"sync/atomic"
	"unsafe"

	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/utils"
)

const (
	// 8 - 1  = 7 字节
	nodeAlign  = int(unsafe.Sizeof(uint64(0))) - 1
	offsetSize = int(unsafe.Sizeof(uint32(0)))
)

// Arena 固定容量分配器: 容量在 NewArena 时一次性确定, 禁止扩容;
// 数据只追加不复用, 已返回的切片在 arena 存活期内永久有效 (不会因扩容迁移而悬垂);
type Arena struct {
	data  []byte
	sizes uint32
}

func NewArena(n int64) *Arena {
	return &Arena{
		data:  make([]byte, n),
		sizes: 1,
	}
}

func (a *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&a.sizes, sz)
	// 固定容量: 调用方 (memtable 轮转预判) 必须保证不越界, 越界即视为内部错误;
	utils.AssertTrue(int(offset) <= len(a.data))
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
