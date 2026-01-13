package cache

import (
	"github.com/kebukeYi/TrainKV/v2/utils"
	"math"
)

type Filter []byte

type BloomFilter struct {
	bitmap Filter
	hashs  uint8
}

func (b *BloomFilter) MayContain(keyHash uint32) bool {
	if b.Len() < 2 {
		return false
	}
	k := b.hashs
	if k > 30 {
		return true
	}
	nBits := uint32(8 * (b.Len() - 1))
	delta := keyHash>>17 | keyHash<<15
	for j := uint8(0); j < k; j++ {
		bitPos := keyHash % nBits
		if b.bitmap[bitPos/8]&(1<<bitPos%8) == 0 {
			return false
		}
		keyHash += delta
	}
	return true
}

func (b *BloomFilter) mayContainKey(key []byte) bool {
	return b.MayContain(utils.Hash(key))
}

func (b *BloomFilter) Insert(keyHash uint32) bool {
	hashs := b.hashs
	nBits := uint32(8 * (b.Len() - 1))
	delta := keyHash>>17 | keyHash<<15
	for i := uint8(0); i < hashs; i++ {
		bitPos := keyHash % nBits
		b.bitmap[bitPos/8] |= 1 << (bitPos % 8)
		keyHash += delta
	}
	return true
}

func (b *BloomFilter) AllowKey(key []byte) bool {
	if b == nil {
		return false
	}
	already := b.mayContainKey(key)
	if !already {
		keyToHash, _ := utils.KeyToHash(key)
		b.Insert(uint32(keyToHash))
	}
	return already
}

func (b *BloomFilter) Allow(keyHash uint32) bool {
	if b == nil {
		return false
	}
	already := b.MayContain(keyHash)
	if !already {
		b.Insert(keyHash)
	}
	return already
}

func (b *BloomFilter) reset() {
	if b == nil {
		return
	}
	for i := range b.bitmap {
		b.bitmap[i] = 0
	}
}

func (b *BloomFilter) Len() int32 {
	return int32(len(b.bitmap))
}

func newBloomFilter(numEntries int, falsePositive float64) *BloomFilter {
	bitsPerKey := bloomBitsPerKey(numEntries, falsePositive)
	return initBloomFilter(numEntries, bitsPerKey)
}

func initBloomFilter(numEntries int, bitsPerKey int) *BloomFilter {
	bf := &BloomFilter{}
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}

	hashs := uint32(float64(bitsPerKey) * 0.69)
	if hashs < 1 {
		hashs = 1
	}
	if hashs > 30 {
		hashs = 30
	}
	bf.hashs = uint8(hashs)

	nBits := numEntries * bitsPerKey
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	filter := make([]byte, nBytes+1)

	filter[nBytes] = uint8(hashs)
	bf.bitmap = filter

	return bf
}

func bloomBitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(0.69314718056, 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}
