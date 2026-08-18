package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/file"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/utils"
)

const (
	WalHeaderSize int    = 21
	crcSize       int    = 4
	walFileExt    string = ".wal"
)

type WAL struct {
	file     *file.MmapFile
	opt      *utils.FileOptions
	lock     sync.Mutex
	buf      *bytes.Buffer
	crcTable *crc32.Table // 写路径经 handleWriteCh 串行化, 无需每记录新建哈希对象;
	writeAt  uint32
	readAt   uint32
}

func OpenWalFile(opt *utils.FileOptions) *WAL {
	mmapFile, err := file.OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		common.Panic(fmt.Errorf("open wal mmap_file error: %v ;\n", err))
	}
	return &WAL{
		file:     mmapFile,
		writeAt:  0,
		opt:      opt,
		buf:      &bytes.Buffer{},
		crcTable: common.CastigationCryTable,
	}
}

func (w *WAL) Write(e *model.Entry) error {
	w.buf.Reset()
	size, err := w.WalEncode(w.buf, e)
	if err != nil {
		return err
	}
	err = w.file.AppendBuffer(w.writeAt, w.buf.Bytes())
	if err != nil {
		return err
	}
	atomic.AddUint32(&w.writeAt, uint32(size))
	return nil
}

// Read 从 wal 文件按顺序读取下一条 entry;
// 文件尾部的零填充区或撕裂的半写记录被视为正常结束(回退到上一条完整记录处),
// 而文件中间出现的解析/校验错误说明存在真实损坏, 直接 panic;
func (w *WAL) Read(reader io.Reader) (*model.Entry, uint32) {
	entry, err := w.WalDecode(reader)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF || err == common.ErrTruncate {
			return nil, 0
		}
		// crc 不匹配: 若其后全是零填充, 则是崩溃时最后一条未完整落盘的记录, 也视为尾部;
		if err == common.ErrWalInvalidCrc {
			rest, readErr := io.ReadAll(reader)
			if readErr == nil && allZero(rest) {
				return nil, 0
			}
		}
		common.Panic(err)
	}
	return entry, w.readAt
}

func allZero(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

// WalEncode | header(meta,klen,vlen,expir) | key | value | crc32 |
func (w *WAL) WalEncode(buf *bytes.Buffer, e *model.Entry) (int, error) {
	header := model.EntryHeader{
		KLen:      uint32(len(e.Key)),
		VLen:      uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
		Meta:      e.Meta,
	}
	var headerEnc [WalHeaderSize]byte
	sz := header.Encode(headerEnc[:])

	buf.Write(headerEnc[:sz])
	buf.Write(e.Key)
	buf.Write(e.Value)

	var crcBuf [crcSize]byte
	// 整条记录一次算出 crc; 若走 hash.Hash 接口逐段 Write, headerEnc 会经接口逃逸到堆;
	binary.BigEndian.PutUint32(crcBuf[:], crc32.Checksum(buf.Bytes(), w.crcTable))
	buf.Write(crcBuf[:])
	return sz + len(e.Key) + len(e.Value) + len(crcBuf), nil
}

func (w *WAL) WalDecode(reader io.Reader) (*model.Entry, error) {
	var err error
	hashReader := model.NewHashReader(reader)
	var header model.EntryHeader
	headLen, err := header.DecodeFrom(hashReader)
	if header.KLen == 0 {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	// todo sync.pool
	entry := &model.Entry{}

	dataBuf := make([]byte, header.KLen+header.VLen)
	dataLen, err := io.ReadFull(hashReader, dataBuf[:])
	if err != nil {
		if err == io.EOF {
			err = common.ErrTruncate
		}
		return nil, err
	}

	entry.Key = dataBuf[:header.KLen]
	entry.Value = dataBuf[header.KLen:]
	entry.Meta = header.Meta
	entry.ExpiresAt = header.ExpiresAt
	sum32 := hashReader.Sum32()

	// 读取 crc32
	crcBuf := make([]byte, crcSize)
	crcLen, err := io.ReadFull(reader, crcBuf[:])
	if err != nil {
		return nil, err
	}
	readChecksumIEEE := binary.BigEndian.Uint32(crcBuf[:])
	if readChecksumIEEE != sum32 {
		return nil, common.ErrWalInvalidCrc
	}
	w.readAt += uint32(headLen + dataLen + crcLen)
	return entry, nil
}

// EstimateWalEncodeSize WalEncode | header(klen,vlen,meta,expir) | key | value | crc32 |
func EstimateWalEncodeSize(e *model.Entry) int {
	return WalHeaderSize + len(e.Key) + len(e.Value) + crcSize // crc 4B
}

func (w *WAL) Size() uint32 {
	return atomic.LoadUint32(&w.writeAt)
}

func (w *WAL) SyncFile() error {
	// WAL 文件被预截断到 MemTableSize, 只同步已写入前缀即可, 整区 msync 会拖慢每次提交;
	return w.file.SyncRange(w.Size())
}

func (w *WAL) SetSize(offset uint32) {
	atomic.StoreUint32(&w.writeAt, offset)
}

func (w *WAL) Fid() uint64 {
	return w.opt.FID
}

func (w *WAL) CloseAndRemove() error {
	fileName := w.file.Fd.Name()
	if err := w.file.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
}

func (w *WAL) Close() error {
	err := w.file.Truncate(int64(w.writeAt))
	if err != nil {
		return err
	}
	if err = w.file.Close(); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Name() string {
	return w.file.Fd.Name()
}
