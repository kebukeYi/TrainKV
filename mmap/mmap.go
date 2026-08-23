package mmap

import "os"

func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

// Munmap unmaps a previously mapped slice.
func Unmap(b []byte) error {
	return unmmap(b)
}

// Madvise uses the madvise system call to give advise about the use of memory
// when using a slice that is memory-mapped to a file. set the readahead flag to
// false if page references are expected in random order.
func Madvise(b []byte, readahead bool) error {
	return mmapadvise(b, readahead)
}

// MadviseSequential 建议内核按顺序访问处理该映射: 启用更激进的预读与顺序页回收;
// 适合 vlog 这类"顺序写 + 大块顺序读 (GC 扫描/大 value 读)"的文件映射;
func MadviseSequential(b []byte) error {
	return mmapadviseSequential(b)
}

// Msync would call sync on the mmapped data.
func Msync(b []byte) error {
	return msync(b)
}

// Mremap unmmap and mmap
func Mremap(data []byte, size int) ([]byte, error) {
	return remmap(data, size)
}
