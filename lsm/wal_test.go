package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/utils"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

var walTestPath = "/usr/projects_gen_data/goprogendata/trainkvdata/test/wal"

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func getDefaultFileOpt() *utils.FileOptions {
	options := &utils.FileOptions{
		FID:      0,
		FileName: "00001.wal",
		Dir:      walTestPath,
		Path:     walTestPath,
		MaxSz:    3 * 1024,
	}
	options.FileName = filepath.Join(walTestPath, options.FileName)
	return options
}

func TestWAL_Write(t *testing.T) {
	w := OpenWalFile(getDefaultFileOpt())
	for i := 0; i < 10; i++ {
		var entry = model.Entry{
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
		var entry model.Entry
		entry, readAt = w.Read(reader)
		if readAt > 0 {
			fmt.Printf("entry: key:%s val:%s meta:%d exp:%d \n",
				entry.Key, entry.Value, entry.Meta, entry.ExpiresAt)
		} else {
			break
		}
	}
}
