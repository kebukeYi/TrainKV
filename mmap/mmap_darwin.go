//go:build darwin
// +build darwin

package mmap

import (
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Mmap uses the mmap system call to memory-map a file. If writable is true,
// memory protection of the pages is set so that they may be written to as well.
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

func remmap(data []byte, size int) ([]byte, error) {
	return data, nil
}

// Munmap unmaps a previously mapped slice.
func unmmap(b []byte) error {
	return unix.Munmap(b)
}

// This is required because the unix package does not support the madvise system call on OS X.
func mmapadvise(b []byte, readahead bool) error {
	advice := unix.MADV_NORMAL
	if !readahead {
		advice = unix.MADV_RANDOM
	}

	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		return e1
	}
	return nil
}

func mmapadviseSequential(b []byte) error {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)), uintptr(unix.MADV_SEQUENTIAL))
	if e1 != 0 {
		return e1
	}
	return nil
}

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
