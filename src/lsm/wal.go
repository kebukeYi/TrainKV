package lsm

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	errors "trainKv/common"
	"trainKv/file"
	"trainKv/model"
)

const (
	WalHeaderSize int    = 17
	crcSize       int    = 4
	walFileExt    string = ".wal"
)

type WalHeader struct {
	keyLen    uint32 // 4B
	valLen    uint32 // 4B
	Meta      byte   // 1B
	ExpiredAt uint64 // 8B
}

func (h WalHeader) encode(buf []byte) int {
	index := 0
	index += binary.PutUvarint(buf[index:], uint64(h.keyLen))
	index += binary.PutUvarint(buf[index:], uint64(h.valLen))
	index += binary.PutUvarint(buf[index:], uint64(h.Meta))
	index += binary.PutUvarint(buf[index+1:], h.ExpiredAt)
	return index
}

func (h *WalHeader) decode(buf []byte) {
	var index = 0
	kSize, n := binary.Uvarint(buf[index:])
	h.keyLen = uint32(kSize)
	index += n

	vSize, n := binary.Uvarint(buf[index:])
	h.valLen = uint32(vSize)
	index += n

	meta, n := binary.Uvarint(buf[index:])
	h.Meta = byte(meta)
	index += n

	expiredAt, n := binary.Uvarint(buf[index:])
	h.ExpiredAt = expiredAt
}

type WAL struct {
	mux     *sync.Mutex
	file    *file.MmapFile
	opt     *model.FileOptions
	size    uint32
	writeAt uint32
	readAt  uint32
}

func OpenWalFile(opt *model.FileOptions) *WAL {
	//file, err := os.OpenFile(opt.FileName, os.O_CREATE|os.O_RDWR, 0666)
	mmapFile, err := file.OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		return nil
	}
	fileInfo, err := mmapFile.Fd.Stat()
	wal := &WAL{
		file:    mmapFile,
		size:    uint32(fileInfo.Size()),
		writeAt: 0,
		mux:     &sync.Mutex{},
		opt:     opt,
	}
	if err != nil {
		fmt.Printf("open wal file error: %v ;\n", err)
		return nil
	}
	return wal
}

func (w *WAL) Write(e *model.Entry) error {
	w.mux.Lock()
	defer w.mux.Unlock()
	walEncode, size := w.WalEncode(e)
	err := w.file.AppendBuffer(w.writeAt, walEncode)
	if err != nil {
		return err
	}
	w.writeAt += uint32(size)
	return nil
}

func (w *WAL) Read(offset uint32) (*model.Entry, uint32) {
	entry, err := w.WalDecode(offset)
	if err != nil {
		if err == io.EOF {
			return nil, 0
		}
		errors.Panic(err)
		return nil, 0
	}
	return entry, w.readAt
}

// WalEncode | header(klen,vlen,meta,expir) | key | value | crc32 |
func (w *WAL) WalEncode(e *model.Entry) ([]byte, int) {
	header := WalHeader{
		keyLen:    uint32(len(e.Key)),
		valLen:    uint32(len(e.Value)),
		ExpiredAt: e.ExpiresAt,
		Meta:      e.Meta,
	}
	var headerEnc [WalHeaderSize]byte
	sz := header.encode(headerEnc[:])
	buf := make([]byte, 0)
	buf = append(buf, headerEnc[:sz]...)
	buf = append(buf, e.Key...)
	buf = append(buf, e.Value...)
	checksumIEEE := crc32.ChecksumIEEE(buf)
	crcBuf := make([]byte, crcSize)
	binary.BigEndian.PutUint32(crcBuf[:], checksumIEEE)
	buf = append(buf, crcBuf...)
	return buf, len(buf)
}

func (w *WAL) WalDecode(offset uint32) (*model.Entry, error) {
	entry := &model.Entry{}
	headerBuf := make([]byte, WalHeaderSize)
	readN, err := w.file.Read(headerBuf, int64(offset))
	if err != nil {
		return nil, err
	}

	var header WalHeader
	header.decode(headerBuf)
	offset = offset + uint32(readN)

	dataBuf := make([]byte, header.keyLen+header.valLen)
	readN, err = w.file.Read(dataBuf, int64(offset))
	entry.Key = dataBuf[:header.keyLen]
	entry.Value = dataBuf[header.keyLen:]
	entry.Meta = header.Meta
	entry.ExpiresAt = header.ExpiredAt

	currChecksumIEEE := crc32.ChecksumIEEE(append(headerBuf, dataBuf...))
	offset = offset + uint32(readN)

	crcBuf := make([]byte, crcSize)
	readN, err = w.file.Read(crcBuf, int64(offset))
	readChecksumIEEE := binary.BigEndian.Uint32(crcBuf[:])
	if readChecksumIEEE != currChecksumIEEE {
		return nil, errors.ErrWalInvalidCrc
	}
	offset = offset + uint32(readN)
	w.readAt = offset
	return entry, nil
}

// WalEncode | header(klen,vlen,meta,expir) | key | value | crc32 |
func EstimateWalEncodeSize(e *model.Entry) int {
	return WalHeaderSize + len(e.Key) + len(e.Value) + crcSize // crc 4B
}

func (w *WAL) Fid() uint64 {
	return w.opt.FID
}

func (w *WAL) Close() error {
	fileName := w.file.Fd.Name()
	if err := w.file.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
}

func (w *WAL) Name() string {
	return w.file.Fd.Name()
}
