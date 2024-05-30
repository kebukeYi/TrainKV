package lsm

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	errors "trainKv/common"
	"trainKv/interfaces"
	"trainKv/model"
)

const (
	WalHeaderSize int    = 17
	crcSize       int    = 4
	walFileExt    string = ".wal"
)

type WalHeader struct {
	keyLen    uint32
	valLen    uint32
	Meta      byte
	ExpiredAt int64
}

func (h WalHeader) encode() []byte {
	index := 0
	buf := make([]byte, WalHeaderSize)
	index += binary.PutVarint(buf[index:], int64(h.keyLen))
	index += binary.PutVarint(buf[index:], int64(h.valLen))
	buf[index] = h.Meta
	index += binary.PutVarint(buf[index+1:], h.ExpiredAt)
	return buf
}

func (h *WalHeader) decode(buf []byte) {
	var index = 0
	kSize, n := binary.Varint(buf[index:])
	h.keyLen = uint32(kSize)
	index += n

	vSize, n := binary.Varint(buf[index:])
	h.valLen = uint32(vSize)
	index += n

	h.Meta = buf[index]

	expiredAt, n := binary.Varint(buf[index+1:])
	h.ExpiredAt = expiredAt
}

type WAL struct {
	file    *os.File
	opt     interfaces.FileOptions
	size    uint32
	writeAt uint64
	readAt  uint64
}

func OpenWalFile(opt *interfaces.FileOptions) *WAL {
	file, err := os.OpenFile(opt.FileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil
	}
	fileInfo, err := file.Stat()
	wal := &WAL{
		file:    file,
		writeAt: 0,
		size:    uint32(fileInfo.Size()),
	}
	if err != nil {
		return nil
	}
	return wal
}

func (w *WAL) Write(e *model.Entry) error {
	walEncode, size := w.WalEncode(e)
	n, err := w.file.WriteAt(walEncode, int64(w.writeAt))
	if err != nil {
		return err
	}
	if n != size {
		return nil
	}
	w.writeAt += uint64(size)
	return nil
}

func (w *WAL) Read(offset uint64) (*model.Entry, uint64) {
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

func (w *WAL) WalEncode(e *model.Entry) ([]byte, int) {
	header := WalHeader{
		keyLen:    uint32(len(e.Key)),
		valLen:    uint32(len(e.Value)),
		ExpiredAt: e.ExpiresAt,
		Meta:      e.Meta,
	}
	headerBuf := header.encode()
	buf := make([]byte, 0)
	buf = append(buf, headerBuf...)
	buf = append(buf, e.Key...)
	buf = append(buf, e.Value...)
	checksumIEEE := crc32.ChecksumIEEE(buf)
	crcBuf := make([]byte, crcSize)
	binary.LittleEndian.PutUint32(crcBuf[:], checksumIEEE)
	buf = append(buf, crcBuf...)
	return buf, len(buf)
}

func (w *WAL) WalDecode(offset uint64) (*model.Entry, error) {
	entry := &model.Entry{}
	headerBuf := make([]byte, WalHeaderSize)
	readN, err := w.file.ReadAt(headerBuf, int64(offset))
	if err != nil {
		return nil, err
	}

	var header WalHeader
	header.decode(headerBuf)
	offset = offset + uint64(readN)

	dataBuf := make([]byte, header.keyLen+header.valLen)
	readN, err = w.file.ReadAt(dataBuf, int64(offset))
	entry.Key = dataBuf[:header.keyLen]
	entry.Value = dataBuf[header.keyLen:]
	entry.Meta = header.Meta
	entry.ExpiresAt = header.ExpiredAt

	currChecksumIEEE := crc32.ChecksumIEEE(append(headerBuf, dataBuf...))
	offset = offset + uint64(readN)

	crcBuf := make([]byte, crcSize)
	readN, err = w.file.ReadAt(crcBuf, int64(offset))
	readChecksumIEEE := binary.LittleEndian.Uint32(crcBuf[:])
	if readChecksumIEEE != currChecksumIEEE {
		return nil, errors.ErrWalInvalidCrc
	}
	offset = offset + uint64(readN)
	w.readAt = offset
	return entry, nil
}

func (w *WAL) EstimateWalEncodeSize(e *model.Entry) int {
	return len(e.Key) + len(e.Value) + WalHeaderSize + 8 // crc 8B
}

// Fid _
func (wf *WAL) Fid() uint64 {
	return wf.opt.FID
}

// Close _
func (wf *WAL) Close() error {
	fileName := wf.file.Name()
	if err := wf.file.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
}

// Name _
func (wf *WAL) Name() string {
	return wf.file.Name()
}
