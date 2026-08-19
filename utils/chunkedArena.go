package utils

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
