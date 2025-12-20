package model

import (
	"encoding/binary"
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"hash"
	"hash/crc32"
	"io"
	"math/rand"
	"time"
)

type LogEntry func(e *Entry, vp *ValuePtr) error

// Entry  db写入的结构体;
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Meta    byte
	Version uint64

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

// 生成随机字符串作为key和value;
func randStr(length int) string {
	// 包括特殊字符,进行测试
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func BuildBigEntry(key []byte, bigValSize uint64) *Entry {
	value := make([]byte, bigValSize)
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

func BuildEntry() *Entry {
	key := []byte(fmt.Sprintf("%s%s", randStr(16), "12345678"))
	value := []byte(randStr(128))
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
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
	if (e.Meta & common.BitDelete) > 0 {
		return true
	}

	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}
	return e.ExpiresAt <= uint64(time.Now().Unix())
}

func (e *Entry) EstimateSize(valThreshold int64) int {
	if e.ValThreshold == 0 {
		e.ValThreshold = valThreshold
	}
	if int64(len(e.Value)) < valThreshold {
		// 1 for meta.
		return len(e.Key) + len(e.Value) + 1
	} else {
		// 12 for ValuePointer, 1 for meta.
		return len(e.Key) + 12 + 1
	}
}

func (e *Entry) SafeCopy() Entry {
	entry := Entry{}
	entry.Key = SafeCopy(nil, e.Key)
	entry.Value = SafeCopy(nil, e.Value)
	entry.Version = e.Version
	entry.Meta = SafeCopy(nil, []byte{e.Meta})[0]
	entry.ExpiresAt = e.ExpiresAt
	return entry
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

// EntryHeader for wal and vlogFile;
type EntryHeader struct {
	KLen      uint32 // 4-5
	VLen      uint32 // 4-5
	ExpiresAt uint64 // 8|10
	Meta      byte   // 1
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
		H:        crc32.New(common.CastigationCryTable),
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

func (r *HashReader) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	//_, err := r.R.Read(buf)
	_, err := r.Read(buf)
	if err != nil {
		return 0, err
	}
	return buf[0], err
}

func (r *HashReader) Sum32() uint32 {
	return r.H.Sum32()
}
