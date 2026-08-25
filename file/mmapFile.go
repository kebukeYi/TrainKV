package file

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/mmap"
)

type MmapFile struct {
	Fd     *os.File
	Buf    []byte
	BufLen int64
}

func OpenMmapFile(fileName string, flag int, maxSz int32) (*MmapFile, error) {
	fd, err := os.OpenFile(fileName, flag, common.DefaultFileMode)
	if err != nil {
		return nil, fmt.Errorf("unable to open: %s, err:%w", fileName, err)
	}
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	fi, err := fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("unable to open: %s, err:%w", fileName, err)
	}
	fileSize := fi.Size()
	if fileSize == 0 && maxSz > 0 {
		// 说明是新创建文件流程, 进行截断文件;
		if err := fd.Truncate(int64(maxSz)); err != nil {
			return nil, fmt.Errorf("error while truncation, err:%w", err)
		}
		fileSize = int64(maxSz)
	}

	buf, err := mmap.Mmap(fd, writable, fileSize) // Mmap up to file size.
	if err != nil {
		return nil, fmt.Errorf("while mmapping %s with size: %d,err:%w", fd.Name(), fileSize, err)
	}

	// 为了处理文件系统缓存或确保目录元数据正确更新;
	if fileSize == 0 {
		dir, _ := filepath.Split(fileName)
		if err = SyncDir(dir); err != nil {
			return nil, err
		}
	}

	return &MmapFile{
		Buf:    buf,
		Fd:     fd,
		BufLen: fileSize,
	}, err
}

// Read copy data from mapped region(buf) into slice b at offset.
func (m *MmapFile) Read(b []byte, offset int64) (int, error) {
	//if offset < 0 || offset >= m.BufLen {
	if offset < 0 || offset >= int64(len(m.Buf)) {
		return 0, io.EOF
	}
	if offset+int64(len(b)) > int64(len(m.Buf)) {
		return 0, io.EOF
	}
	end := offset + int64(len(b))
	return copy(b, m.Buf[offset:end]), nil
}

// Sync synchronize the mapped buffer to the file's contents on disk.
func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Buf)
}

// SyncRange 只同步映射区 [0, n) 内的脏页; 文件通常被预分配到较大尺寸,
// 整区 msync 需要遍历大量干净页表项, 按已写入长度同步可显著降低延迟;
func (m *MmapFile) SyncRange(n uint32) error {
	if m == nil || n == 0 {
		return nil
	}
	if int64(n) > int64(len(m.Buf)) {
		n = uint32(len(m.Buf))
	}
	return mmap.Msync(m.Buf[:n])
}

// SyncDirtyRange 只同步映射区 [off, n) 内的脏页; 调用方需保证 [0, off) 已在上一次同步完成,
// 供 WAL 每次提交只同步新增区间, 避免对不断增长的已写前缀反复做整区页表扫描;
func (m *MmapFile) SyncDirtyRange(off, n uint32) error {
	if m == nil || n <= off {
		return nil
	}
	if int64(n) > int64(len(m.Buf)) {
		n = uint32(len(m.Buf))
	}
	// 将偏移量 off 向下对齐到操作系统内存页大小的整数倍, 即清除 a 中所有在 b 的二进制位为 1 的对应位
	// 将低 12 位清零，等同于将数字向下舍入到最近的 4096 的倍数;
	// off = 5000（0x1388） → 计算结果为 4096（0x1000）。即把 5000 向下对齐到 4KB 的起始位置;
	// msync 要求起始地址页对齐: 向下取整到页边界, 多同步的一页属于已同步前缀, 无副作用;
	start := off &^ (uint32(os.Getpagesize()) - 1)
	return mmap.Msync(m.Buf[start:n])
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
		// 指数扩容: 一次扩到 end+growBy, 避免每次追加都触发 Truncate(全量sync+remmap);
		// 扩出的空间由 mmap 稀疏页兜底, 不写入的数据仍是零页, 不产生实际磁盘占用;
		growBy := size
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < needSize {
			growBy = needSize
		}
		if err := m.Truncate(int64(end + growBy)); err != nil {
			return err
		}
	}
	dLen := copy(m.Buf[offset:end], buf)
	if dLen != needSize {
		return errors.New("#AppendBuffer dLen != needSize AppendBuffer failed")
	}
	return nil
}

func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}

	if err := mmap.Unmap(m.Buf); err != nil {
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
	if err := mmap.Unmap(m.Buf); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return m.Fd.Close()
}

func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("while opening %s,err:%w", dir, err)
	}
	if err := df.Sync(); err != nil {
		return fmt.Errorf("while syncing %s,err:%w", dir, err)
	}
	if err := df.Close(); err != nil {
		return fmt.Errorf("while closing %s,err:%w", dir, err)
	}
	return nil
}
