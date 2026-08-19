package benchmk

import (
	"math/rand"
	"strconv"
	"time"
)

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func init() {
	rand.Seed(time.Now().Unix())
}

var keyPrefix = []byte("test_key_")

func GetKey(n int) []byte {
	// 预分配容量, 一次分配完成 key 拼装, 避免 fmt.Sprintf 的多次分配污染基准数据;
	key := make([]byte, 0, len(keyPrefix)+9)
	key = append(key, keyPrefix...)
	return strconv.AppendInt(key, int64(n), 10)
}

func GetValue() []byte {
	val := make([]byte, 0, 512)
	for i := 0; i < 512; i++ {
		val = append(val, alphabet[rand.Int()%36])
	}
	return val
}
