package mmap

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"trainKv/common"
)

type MmapFile struct {
	Fd     *os.File
	Buf    []byte
	BufLen int64
}

func OpenMmapFile(fileName string, flag int, maxSz int) (*MmapFile, error) {
	fd, err := os.OpenFile(fileName, flag, common.DefaultFileMode)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", fileName)
	}
	writable := true
	fileSize := 0
	if flag == os.O_RDONLY {
		writable = false
	}
	fileInfo, err := fd.Stat()
	if err == nil && fileInfo != nil && fileInfo.Size() > 0 {
		maxSz = int(fileInfo.Size())
	}
	if maxSz > 0 && fileInfo.Size() == 0 {
		// If file is empty, truncate it to sz.
		if err := fd.Truncate(int64(maxSz)); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
		fileSize = maxSz
	}
	buf, err := Mmap(fd, writable, int64(maxSz))
	if err != nil {
		return nil, errors.Wrapf(err, "while mmapping %s with size: %d", fd.Name(), fileSize)
	}
	if fileSize == 0 {
		dir, _ := filepath.Split(fileName)
		go SyncDir(dir)
	}
	return &MmapFile{
		Buf: buf,
		Fd:  fd,
	}, err
}

func (m *MmapFile) Write(b []byte) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	m.Buf = append(m.Buf, b...)
	return len(b), nil
}

// Read copy data from mapped region(buf) into slice b at offset.
func (m *MmapFile) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= m.BufLen {
		return 0, io.EOF
	}
	if offset+int64(len(b)) >= m.BufLen {
		return 0, io.EOF
	}
	return copy(b, m.Buf[offset:]), nil
}

// Sync synchronize the mapped buffer to the file's contents on disk.
func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return Msync(m.Buf)
}

func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Buf[off:]) < sz {
		return nil, io.EOF
	}
	return m.Buf[off : off+sz], nil
}

const oneGB = 1 << 30

func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	size := len(m.Buf)
	needSize := len(buf)
	end := int(offset) + needSize
	if end > size {
		growBy := size
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < needSize {
			growBy = needSize
		}
		if err := m.Truncature(int64(end)); err != nil {
			return err
		}
	}
	dLen := copy(m.Buf[offset:end], buf)
	if dLen != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}

	if err := Unmap(m.Buf); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	m.Buf = nil
	if err := m.Fd.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return os.Remove(m.Fd.Name())
}

func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := Unmap(m.Buf); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return m.Fd.Close()
}

func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}

func (m *MmapFile) Truncature(maxSz int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}

	var err error
	m.Buf, err = Mremap(m.Buf, int(maxSz)) // Mmap up to max size.
	return err
}
