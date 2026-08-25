package lsm

import (
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
	file       *file.MmapFile
	opt        *utils.FileOptions
	lock       sync.Mutex
	crcTable   *crc32.Table // 写路径经 handleWriteCh 串行化, 无需每记录新建哈希对象;
	writeAt    uint32
	readAt     uint32
	lastSynced uint32 // 已同步的写入前缀; 每次只同步 [lastSynced, writeAt) 新增区间;
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
		crcTable: common.CastigationCryTable,
	}
}

func (w *WAL) Write(e *model.Entry) error {
	// 直接编码进 mmap, 避免 bytes.Buffer 中转的二次拷贝 (与 vlogFile.EncodeEntryAt 同模式);
	size, err := w.EncodeAt(e, atomic.LoadUint32(&w.writeAt))
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

// EncodeAt | header(meta,klen,vlen,expir) | key | value | crc32 |
// 与 vlogFile.EncodeEntryAt 相同的布局; 直接编码进 mmap 指定偏移, 返回编码总长;
func (w *WAL) EncodeAt(e *model.Entry, offset uint32) (int, error) {
	header := model.EntryHeader{
		KLen:      uint32(len(e.Key)),
		VLen:      uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
		Meta:      e.Meta,
	}
	var headerEnc [WalHeaderSize]byte
	encodeLen := header.Encode(headerEnc[:])
	total := encodeLen + len(e.Key) + len(e.Value) + crcSize
	if int(offset)+total > len(w.file.Buf) {
		// 预分配(MemTableSize)不足时扩容; 轮转预判按 arena 上界估算, 正常不会触发;
		if err := w.file.Truncate(int64(offset) + int64(total)); err != nil {
			return 0, err
		}
	}
	dst := w.file.Buf[offset:]
	n := copy(dst, headerEnc[:encodeLen])
	n += copy(dst[n:], e.Key)
	n += copy(dst[n:], e.Value)
	// 对已写区段整体算 crc (数据刚写入, 读回在缓存中);
	binary.BigEndian.PutUint32(dst[n:], crc32.Checksum(dst[:n], w.crcTable))
	return total, nil
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

func (w *WAL) Size() uint32 {
	return atomic.LoadUint32(&w.writeAt)
}

func (w *WAL) SyncFile() error {
	// WAL 文件被预截断到 MemTableSize, 只需同步新增区间 [lastSynced, Size):
	// 已同步前缀无需重复 msync, 整区同步会随 WAL 增长线性拖慢每次提交;
	size := w.Size()
	if size <= w.lastSynced {
		return nil
	}
	if err := w.file.SyncDirtyRange(w.lastSynced, size); err != nil {
		return err
	}
	w.lastSynced = size
	return nil
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
