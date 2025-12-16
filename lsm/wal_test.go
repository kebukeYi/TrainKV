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

var walTestPath = "/usr/golanddata/trainkv/wal"

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func getDefaultFileOpt(walFilePath string) *utils.FileOptions {
	opt := GetDefaultOpt("")
	options := &utils.FileOptions{
		FID:      0,
		FileName: walFilePath,
		Dir:      walFilePath,
		Path:     walFilePath,
		MaxSz:    int32(opt.MemTableSize),
	}
	//options.FileName = filepath.Join(walTestPath, options.FileName)
	return options
}
func TestWAL_WalDecode(t *testing.T) {
	walFilePath := filepath.Join(walTestPath, "0.00005.wal")
	w := OpenWalFile(getDefaultFileOpt(walFilePath))
	reader := model.NewHashReader(w.file.Fd)
	var readAt uint32 = 0
	for {
		var entry *model.Entry
		entry, readAt = w.Read(reader)
		if readAt > 0 {
			fmt.Printf("entry: key:%s val:%s meta:%d version:%d \n",
				model.ParseKey(entry.Key), entry.Value, entry.Meta, model.ParseTsVersion(entry.Key))
		} else {
			break
		}
	}
}

func TestWAL_Write(t *testing.T) {
	w := OpenWalFile(getDefaultFileOpt(""))
	for i := 0; i < 10; i++ {
		var entry = model.Entry{
			Key:       []byte(RandString(10)),
			Value:     []byte(RandString(10)),
			Meta:      1,
			ExpiresAt: uint64(time.Now().Unix()),
		}
		err := w.Write(&entry)
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
