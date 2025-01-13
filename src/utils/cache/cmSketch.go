package cache

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	cmDepth = 4
)

// 使用多个计数器,因为仅仅单个计数器的值存在误差;
type cmSketch struct {
	rows [cmDepth]cmRow  // 4个计数器, 每次只取其中最小计数器的的相关key的频率值;
	seed [cmDepth]uint64 // hash遍布的更随机;
	mask uint64          // mask 一定是0111...111
}

func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid numCounters!")
	}
	// numCounters 一定是二次幂，也就一定是1后面有 n 个 0
	numCounters = next2power(numCounters)
	// mask 一定是0111...111
	sketch := &cmSketch{mask: uint64(numCounters - 1)}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))
	// 初始化4行
	// 0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000
	for i := 0; i < cmDepth; i++ {
		sketch.seed[i] = source.Uint64()
		sketch.rows[i] = NewCmRow(numCounters)
	}
	return sketch
}

func (cs *cmSketch) increment(keyHash uint64) {
	for i := 0; i < cmDepth; i++ {
		cs.rows[i].increment((keyHash ^ cs.seed[i]) & cs.mask)
	}
}

// Estimate 估计keyHash的频率值
func (cs *cmSketch) Estimate(keyHash uint64) uint8 {
	min := uint8(255)
	for i := 0; i < cmDepth; i++ {
		val := cs.rows[i].get((keyHash ^ cs.seed[i]) & cs.mask)
		if val < min {
			min = val
		}
	}
	return min
}

func (cs *cmSketch) Reset() {
	for i := 0; i < cmDepth; i++ {
		cs.rows[i].reset()
	}
}

func (cs *cmSketch) Clear() {
	for i := 0; i < cmDepth; i++ {
		cs.rows[i].clear()
	}
}

// 快速计算大于 X，且最接近 X 的二次幂
func next2power(numCounters int64) int64 {
	numCounters--
	numCounters |= numCounters >> 1
	numCounters |= numCounters >> 2
	numCounters |= numCounters >> 4
	numCounters |= numCounters >> 8
	numCounters |= numCounters >> 16
	numCounters |= numCounters >> 32
	numCounters++
	return numCounters
}

// 计数器, 每byte,8位记录了两个数字的频率值, 高四位代表奇数,低四位代表偶数;
type cmRow []byte

func NewCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

func (cr cmRow) get(keyHash uint64) byte {
	return cr[keyHash/2] >> ((keyHash & 1) * 4) & 0x0f
}

func (cr cmRow) increment(keyHash uint64) {
	index := keyHash / 2
	offset := (keyHash & 1) * 4
	val := (cr[index] >> offset) & 0x0f
	if val < 15 {
		cr[index] += 1 << offset
	}
}

func (cr cmRow) reset() {
	for i := range cr {
		cr[i] = (cr[i] >> 1) & 0xf7
	}
}

func (cr cmRow) clear() {
	for i := range cr {
		cr[i] = 0
	}
}

func (cr cmRow) string() string {
	s := ""
	for i := uint64(0); i < uint64(len(cr)*2); i++ {
		s += fmt.Sprintf("%02x", cr.get(i))
	}
	s = s[:len(s)-1]
	return s
}
