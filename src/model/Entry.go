package model

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"time"
	"trainKv/common"
)

type LogEntry func(e *Entry, vp *ValuePtr) error

// Entry _ 最外层写入的结构体
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Meta         byte
	Version      uint64
	HeaderLen    int
	Offset       uint32
	ValThreshold int64
}

func NewEntry(key, val []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: val,
	}
}

func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

func (e *Entry) EncodeSize() uint32 {
	valLen := len(e.Value)
	varIntLen := sizeVarInt(uint64(e.Meta))
	ExpiresAtLen := sizeVarInt(e.ExpiresAt)
	return uint32(valLen + varIntLen + ExpiresAtLen)
}

func (e *Entry) IsDeleteOrExpired() bool {
	if e.Value == nil {
		return true
	}

	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

func (e *Entry) EstimateSize(valThreshold int) int {
	if len(e.Value) < valThreshold {
		// 1 for meta.
		return len(e.Key) + len(e.Value) + 1
	} else {
		// 12 for ValuePointer, 1 for meta.
		return len(e.Key) + 12 + 1
	}
}

func sizeVarInt(a uint64) (n int) {
	for {
		n++
		a >>= 7
		if a == 0 {
			break
		}
	}
	return n
}

type EntryHeader struct {
	KLen      uint32
	VLen      uint32
	ExpiresAt uint64
	Meta      byte
}

func (h EntryHeader) Encode(out []byte) int {
	out[0] = h.Meta
	index := 1
	index += binary.PutUvarint(out[index:], uint64(h.KLen))
	index += binary.PutUvarint(out[index:], uint64(h.VLen))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}
func (h *EntryHeader) Decode(buf []byte) int {
	h.Meta = buf[0]
	index := 1
	klen, count := binary.Uvarint(buf[index:])
	h.KLen = uint32(klen)
	index += count

	vlen, count := binary.Uvarint(buf[index:])
	h.VLen = uint32(vlen)
	index += count

	h.ExpiresAt, count = binary.Uvarint(buf[index:])
	return index + count
}

func (h *EntryHeader) DecodeFrom(reader *HashReader) (int, error) {
	var err error
	h.Meta, err = reader.ReadByte()
	if err != nil {
		return 0, err
	}
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KLen = uint32(klen)

	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.VLen = uint32(vlen)

	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.ByteRead, nil
}

type HashReader struct {
	R        io.Reader
	H        hash.Hash32
	ByteRead int
}

func NewHashReader(read io.Reader) *HashReader {
	return &HashReader{
		R:        read,
		H:        crc32.New(common.CastagnoliCrcTable),
		ByteRead: 0,
	}
}

func (r *HashReader) Read(out []byte) (int, error) {
	n, err := r.R.Read(out)
	if err != nil {
		return n, err
	}
	r.ByteRead += n
	return r.H.Write(out[:n])
}

func (r HashReader) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	_, err := r.R.Read(buf)
	if err != nil {
		return 0, err
	}
	return buf[0], err
}
func (r *HashReader) Sum32() uint32 {
	return r.H.Sum32()
}
