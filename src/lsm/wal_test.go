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
		FileName: "/usr/local/go_temp_files/test/trainKV/waltest/00001.wal",
		MaxSz:    3 * 1024,
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
	w.file.Sync()
	fmt.Printf("writeAt: %d\n", w.writeAt)
	reader := model.NewHashReader(w.file.Fd)
	var readAt uint32 = 0
	for {
		var entry *model.Entry
		entry, readAt = w.Read(reader)
		if readAt > 0 {
			fmt.Printf("entry: key:%s val:%s meta:%d exp:%d \n",
				entry.Key, entry.Value, entry.Meta, entry.ExpiresAt)
		} else {
			break
		}
	}
}
