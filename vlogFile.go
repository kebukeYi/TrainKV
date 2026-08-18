package TrainKV

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/file"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/utils"
	"github.com/pkg/errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
)

type VLogFile struct {
	f    *file.MmapFile
	FID  uint32
	size uint32
	Lock sync.RWMutex
	opt  *lsm.Options
}

func (vlog *VLogFile) Open(opt *utils.FileOptions) error {
	var err error
	vlog.FID = uint32(opt.FID)
	vlog.Lock = sync.RWMutex{}
	vlog.f, err = file.OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		return errors.Wrapf(err, "Unable to open mmap file: %q", opt.FileName)
	}
	info, err := vlog.f.Fd.Stat()
	if err != nil {
		return common.WarpErr("#Open Unable to run VLogFile.Stat", err)
	}
	vlog.size = uint32(info.Size()) // 看最终截断的长度;
	common.CondPanic(vlog.size > math.MaxUint32, fmt.Errorf("file size: %d greater than %d", vlog.size, uint32(math.MaxUint32)))
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
			return errors.Wrapf(err, "Unable to sync value log: %q", vlog.FileName())
		}
	}

	// 确保在"取消映射→重新映射"这个关键操作期间，没有其他线程能访问这个内存区域;
	vlog.Lock.Lock()
	defer vlog.Lock.Unlock()

	if err := vlog.f.Truncate(int64(offset)); err != nil {
		return errors.Wrapf(err, "Unable to truncate file: %q", vlog.FileName())
	}

	if err := vlog.Init(); err != nil {
		return errors.Wrapf(err, "failed to initialize file %s", vlog.FileName())
	}
	return nil
}

func (vlog *VLogFile) Write(offset uint32, buf []byte) (err error) {
	return vlog.f.AppendBuffer(offset, buf)
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
		return errors.Wrapf(err, "Unable to check stat for %q", vlog.FileName())

	}
	size := info.Size()
	if size == 0 {
		return nil
	}
	common.CondPanic(size > math.MaxUint32, fmt.Errorf("[LogFile.Init] sz > math.MaxUint32"))
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

// Sync You must hold lf.lock to sync()
func (vlog *VLogFile) Sync() error {
	return vlog.f.SyncRange(atomic.LoadUint32(&vlog.size))
}

func (vlog *VLogFile) Close() error {
	return vlog.f.Close()
}

// EncodeEntry will encode entry to the out
// layout of entry in vlogFile;
// +----------------------------------+-----+-------+-------+
// | header(meta,klen,vlen,ExpiresAt) | key | value | crc32 |
// +----------------------------------+-----+-------+-------+
func (vlog *VLogFile) EncodeEntry(entry *model.Entry, out *bytes.Buffer) (int, error) {
	header := model.EntryHeader{
		KLen:      uint32(len(entry.Key)),
		VLen:      uint32(len(entry.Value)),
		ExpiresAt: entry.ExpiresAt,
		Meta:      entry.Meta,
	}

	var headerBuf [common.MaxHeaderSize]byte
	encodeLen := header.Encode(headerBuf[:])
	start := out.Len()
	out.Write(headerBuf[:encodeLen])
	out.Write(entry.Key)
	out.Write(entry.Value)

	// 一次性对已编码字节算 crc; 若走 hash.Hash 接口逐段 Write, headerBuf 等会经接口逃逸到堆;
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc32.Checksum(out.Bytes()[start:], common.CastigationCryTable))
	out.Write(crcBuf[:])

	return encodeLen + len(entry.Key) + len(entry.Value) + len(crcBuf), nil
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
