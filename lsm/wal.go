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
	index = binary.PutUvarint(buf[index:], uint64(h.keyLen))
	index += binary.PutUvarint(buf[index:], uint64(h.valLen))
	index += binary.PutUvarint(buf[index:], uint64(h.Meta))
	index += binary.PutUvarint(buf[index:], h.ExpiredAt)
	return index
}

func (h *WalHeader) decode(reader *model.HashReader) (int, error) {
	var err error
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.keyLen = uint32(klen)

	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.valLen = uint32(vlen)

	meta, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.Meta = byte(meta)
	h.ExpiredAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.ByteRead, nil
}

type WAL struct {
	file    *file.MmapFile
	opt     *utils.FileOptions
	lock    sync.Mutex
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
	}
	if err != nil {
		fmt.Printf("open wal file error: %v ;\n", err)
		return nil
	}
	return wal
}

func (w *WAL) Write(e *model.Entry) error {
	walEncode, size := w.WalEncode(e)
	w.lock.Lock()
	defer w.lock.Unlock()
	err := w.file.AppendBuffer(w.writeAt, walEncode)
	if err != nil {
		return err
	}
	w.writeAt += uint32(size)
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

// WalEncode | header(klen,vlen,meta,expir) | key | value | crc32 |
func (w *WAL) WalEncode(e *model.Entry) ([]byte, int) {
	header := WalHeader{
		keyLen:    uint32(len(e.Key)),
		valLen:    uint32(len(e.Value)),
		ExpiredAt: e.ExpiresAt,
		Meta:      e.Meta,
	}
	buf := &bytes.Buffer{}
	var headerEnc [WalHeaderSize]byte
	sz := header.encode(headerEnc[:])

	hash := crc32.New(errors.CastigationCryTable)
	writer := io.MultiWriter(buf, hash)

	writer.Write(headerEnc[:sz])
	writer.Write(e.Key)
	writer.Write(e.Value)

	crcBuf := make([]byte, crcSize)
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	writer.Write(crcBuf[:])
	return buf.Bytes(), len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf)
}

func (w *WAL) WalDecode(reader io.Reader) (*model.Entry, error) {
	var err error
	hashReader := model.NewHashReader(reader)
	var header WalHeader
	headLen, err := header.decode(hashReader)
	if header.keyLen == 0 {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	entry := &model.Entry{}

	dataBuf := make([]byte, header.keyLen+header.valLen)
	dataLen, err := io.ReadFull(hashReader, dataBuf[:])
	if err != nil {
		if err == io.EOF {
			err = errors.ErrTruncate
		}
		return nil, err
	}
	entry.Key = dataBuf[:header.keyLen]
	entry.Value = dataBuf[header.keyLen:]
	entry.Meta = header.Meta
	entry.ExpiresAt = header.ExpiredAt
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
	return w.writeAt
}

func (w *WAL) SetSize(offset uint32) {
	w.writeAt = offset
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
