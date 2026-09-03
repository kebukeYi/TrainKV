package TrainKV

import (
	"encoding/binary"
	"fmt"

	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/file"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/kebukeYi/TrainKV/v2/mmap"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/utils"
)

type VLogFile struct {
	f          *file.MmapFile
	FID        uint32
	size       uint32
	lastSynced uint32 // 已同步的写入前缀; 每次只同步 [lastSynced, size) 新增区间;
	Lock       sync.RWMutex
	opt        *lsm.Options
}

func (vlog *VLogFile) Open(opt *utils.FileOptions) error {
	var err error
	vlog.FID = uint32(opt.FID)
	vlog.Lock = sync.RWMutex{}
	vlog.f, err = file.OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		return fmt.Errorf("unable to open mmap file: %q,err:%w", opt.FileName, err)
	}
	// 读侧 hint: vlog 访问以顺序为主 (GC 全文件扫描、大 value 顺序读), 启用更激进预读与顺序页回收;
	// 提示失败不致命, 退回内核默认行为;
	if err = mmap.MadviseSequential(vlog.f.Buf); err != nil {
		common.Err(err)
	}
	info, err := vlog.f.Fd.Stat()
	if err != nil {
		return common.WarpErr("#Open Unable to run VLogFile.Stat", err)
	}
	vlog.size = uint32(info.Size()) // 看最终截断的长度;
	common.CondPanicf(vlog.size > math.MaxUint32,
		"file size: %d greater than %d \n", vlog.size, uint32(math.MaxUint32))
	return nil
}

func (vlog *VLogFile) Read(vptr *model.ValuePtr) (buf []byte, err error) {
	offset := vptr.Offset
	size := int64(len(vlog.f.Buf))
	valLen := vptr.Len
	vlogSize := atomic.LoadUint32(&vlog.size)
	if int64(offset) >= size || int64(offset+vptr.Len) > size || int64(offset+valLen) > int64(vlogSize) {
		err = io.EOF
	} else {
		buf, err = vlog.f.Bytes(int(offset), int(valLen))
	}
	return buf, err
}

func (vlog *VLogFile) DoneWriting(offset uint32) error {
	if vlog.opt.SyncWrites {
		// vlog 文件被预分配到 MaxSz, 只同步即将截断保留的 [0, offset) 区间;
		if err := vlog.f.SyncRange(offset); err != nil {
			return fmt.Errorf("unable to sync value log: %q,err:%w", vlog.FileName(), err)
		}
	}
	// 全区间已同步, 之后不再对该文件做增量同步;
	vlog.lastSynced = offset

	// 确保在"取消映射→重新映射"这个关键操作期间，没有其他线程能访问这个内存区域;
	vlog.Lock.Lock()
	defer vlog.Lock.Unlock()

	if err := vlog.f.Truncate(int64(offset)); err != nil {
		return fmt.Errorf("unable to truncate file: %q,err:%w", vlog.FileName(), err)
	}

	if err := vlog.Init(); err != nil {
		return fmt.Errorf("failed to initialize file %s,err:%w", vlog.FileName(), err)
	}
	return nil
}

func (vlog *VLogFile) Truncate(offset int64) error {
	return vlog.f.Truncate(offset)
}

func (vlog *VLogFile) Size() int64 {
	return int64(atomic.LoadUint32(&vlog.size))
}

func (vlog *VLogFile) SetSize(offset uint32) {
	atomic.StoreUint32(&vlog.size, offset)
}

func (vlog *VLogFile) Init() error {
	info, err := vlog.f.Fd.Stat()
	if err != nil {
		return fmt.Errorf("unable to check stat for %q,err:%w", vlog.FileName(), err)

	}
	size := info.Size()
	if size == 0 {
		return nil
	}
	common.CondPanicf(size > math.MaxUint32, "[vLogFile.Init()] info.size:%d > math.MaxUint32;\n", size)
	vlog.size = uint32(size)
	return nil
}

func (vlog *VLogFile) FileName() string {
	return vlog.f.Fd.Name()
}

func (vlog *VLogFile) Seek(offset int64, whence int) (ret int64, err error) {
	return vlog.f.Fd.Seek(offset, whence)
}

func (vlog *VLogFile) FD() *os.File {
	return vlog.f.Fd
}

// Sync 只同步映射区 [lastSynced, size) 内的新增脏页;
// 文件预分配到大尺寸, 整区 msync 需遍历大量干净页表项, 增量同步可显著降低提交延迟;
func (vlog *VLogFile) Sync() error {
	size := atomic.LoadUint32(&vlog.size)
	if size <= vlog.lastSynced {
		return nil
	}
	if err := vlog.f.SyncDirtyRange(vlog.lastSynced, size); err != nil {
		return err
	}
	vlog.lastSynced = size
	return nil
}

func (vlog *VLogFile) Close() error {
	return vlog.f.Close()
}

// EncodeEntryAt 将 entry 直接编码进 mmap 的指定偏移 (含 crc32), 返回编码总长;
// 相比先写 bytes.Buffer 再拷入 mmap, 大 value 少一次整段拷贝;
// layout of entry in vlogFile;
// +----------------------------------+-----+-------+-------+
// | header(meta,klen,vlen,ExpiresAt) | key | value | crc32 |
// +----------------------------------+-----+-------+-------+
func (vlog *VLogFile) EncodeEntryAt(entry *model.Entry, offset uint32) (int, error) {
	header := model.EntryHeader{
		KLen:      uint32(len(entry.Key)),
		VLen:      uint32(len(entry.Value)),
		ExpiresAt: entry.ExpiresAt,
		Meta:      entry.Meta,
	}

	var headerBuf [common.MaxHeaderSize]byte
	encodeLen := header.Encode(headerBuf[:])
	total := encodeLen + len(entry.Key) + len(entry.Value) + crc32.Size
	if int(offset)+total > len(vlog.f.Buf) {
		// mmap 预分配容量不足时扩容: 一次扩到创建时的默认预分配尺寸(2*ValueLogFileSize),
		// 而不是只扩到本条记录末尾; 否则关库截断后重开, 映射紧贴数据末尾, 每追加一条都
		// 触发 Truncate(整文件 msync + ftruncate + mremap), 写入退化为全文件同步;
		growTo := int64(offset) + int64(total)
		if defaultSize := int64(vlog.opt.ValueLogFileSize) * 2; defaultSize > growTo {
			growTo = defaultSize
		}
		if err := vlog.f.Truncate(growTo); err != nil {
			return 0, err
		}
	}
	dst := vlog.f.Buf[offset:]
	n := copy(dst, headerBuf[:encodeLen])
	n += copy(dst[n:], entry.Key)
	n += copy(dst[n:], entry.Value)
	// 对已写区段整体算 crc (数据刚写入, 读回在缓存中);
	binary.BigEndian.PutUint32(dst[n:], crc32.Checksum(dst[:n], common.CastigationCryTable))
	return total, nil
}

func (vlog *VLogFile) DecodeEntry(buf []byte, offset uint32) (*model.Entry, error) {
	var header model.EntryHeader
	decodeLen := header.Decode(buf)
	kv := buf[decodeLen:]
	e := &model.Entry{
		Key:       kv[:header.KLen],
		Value:     kv[header.KLen : header.KLen+header.VLen],
		ExpiresAt: header.ExpiresAt,
		Meta:      header.Meta,
		Offset:    offset,
	}
	return e, nil
}
