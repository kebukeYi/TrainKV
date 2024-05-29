package model

import "time"

type Entry struct {
	Key       []byte
	Value     []byte
	Mete      byte
	ExpiresAt uint64
	Version   uint64
	HeaderLen int
	Offset    uint32
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

func (e Entry) EncodeSize() uint32 {
	keyLen := len(e.Key)
	valLen := len(e.Value)
	varIntLen := sizeVarInt(uint64(e.Mete))
	ExpiresAtLen := sizeVarInt(uint64(e.ExpiresAt))
	return uint32(keyLen + valLen + varIntLen + ExpiresAtLen)
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
