package lsm

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	"trainKv/model"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

// /usr/local/temp/train/wal
// /usr/local/temp/train/tables
func TestWAL_Write(t *testing.T) {
	w := OpenWalFile(&model.FileOptions{
		FileName: "/user/local/temp/trainkv/wal/1.wal",
	})
	for i := 0; i < 10; i++ {
		var entry = &model.Entry{
			Key:       []byte(RandString(10)),
			Value:     []byte(RandString(10)),
			Meta:      1,
			ExpiresAt: uint64(time.Now().Unix()),
		}
		err := w.Write(entry)
		if err != nil {
			fmt.Printf("write failed, err: %v\n", err)
		}
	}

	fmt.Printf("writeAt: %d\n", w.readAt)

	var readAt uint64 = 0
	for {
		var entry *model.Entry
		entry, readAt = w.Read(readAt)
		if readAt > 0 {
			fmt.Printf("entry: %v\n", entry)
		} else {
			break
		}
	}
}
