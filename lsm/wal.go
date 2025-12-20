package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	errors "github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/file"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/utils"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

const (
	WalHeaderSize int    = 21
	crcSize       int    = 4
	walFileExt    string = ".wal"
)

type WAL struct {
	file    *file.MmapFile
	opt     *utils.FileOptions
	lock    sync.Mutex
	buf     *bytes.Buffer
	size    uint32
	writeAt uint32
	readAt  uint32
}

func OpenWalFile(opt *utils.FileOptions) *WAL {
	mmapFile, err := file.OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		return nil
	}
	fileInfo, err := mmapFile.Fd.Stat()
	wal := &WAL{
		file:    mmapFile,
		size:    uint32(fileInfo.Size()),
		writeAt: 0,
		opt:     opt,
		buf:     &bytes.Buffer{},
	}
	if err != nil {
		fmt.Printf("open wal file error: %v ;\n", err)
		return nil
	}
	return wal
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

func (w *WAL) Read(reader io.Reader) (*model.Entry, uint32) {
	entry, err := w.WalDecode(reader)
	if err != nil {
		if err == io.EOF {
			return nil, 0
		}
		errors.Panic(err)
	}
	return entry, w.readAt
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

	hash := crc32.New(errors.CastigationCryTable)
	writer := io.MultiWriter(buf, hash)

	writer.Write(headerEnc[:sz])
	writer.Write(e.Key)
	writer.Write(e.Value)

	crcBuf := make([]byte, crcSize)
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	writer.Write(crcBuf[:])
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf), nil
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
	entry := &model.Entry{}

	dataBuf := make([]byte, header.KLen+header.VLen)
	dataLen, err := io.ReadFull(hashReader, dataBuf[:])
	if err != nil {
		if err == io.EOF {
			err = errors.ErrTruncate
		}
		return nil, err
	}
	entry.Key = dataBuf[:header.KLen]
	entry.Value = dataBuf[header.KLen:]
	entry.Meta = header.Meta
	entry.ExpiresAt = header.ExpiresAt
	sum32 := hashReader.Sum32()

	crcBuf := make([]byte, crcSize)
	crcLen, err := io.ReadFull(reader, crcBuf[:])
	if err != nil {
		return nil, err
	}
	readChecksumIEEE := binary.BigEndian.Uint32(crcBuf[:])
	if readChecksumIEEE != sum32 {
		return nil, errors.ErrWalInvalidCrc
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
