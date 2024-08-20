package src

import (
	"fmt"
	"os"
	"testing"
	"time"
	"trainKv/common"
	"trainKv/model"
)

var dbOption = &DBOptions{
	//ValueThreshold:      common.DefaultValueThreshold,
	ValueThreshold:      0,
	WorkDir:             "/usr/local/go_temp_files/test/trainKV/dbtest",
	MemTableSize:        1 << 10,
	SSTableSize:         1 << 10,
	MaxBatchCount:       10,
	MaxBatchSize:        1 << 20,
	ValueLogFileSize:    1 << 20,
	MaxValueLogFileSize: common.MaxValueLogSize,
	VerifyValueChecksum: false,
	ValueLogMaxEntries:  100,
	LogRotatesToFlush:   1000,
	MaxTableSize:        1000,
}

func clearDir() {
	_, err := os.Stat(dbOption.WorkDir)
	if err == nil {
		if err = os.RemoveAll(dbOption.WorkDir); err != nil {
			common.Panic(err)
		}
	}
	err = os.Mkdir(dbOption.WorkDir, os.ModePerm)
	if err != nil {
		_ = fmt.Sprintf("create dir %s failed", dbOption.WorkDir)
	}
}

func TestAPI(t *testing.T) {
	clearDir()
	trainKVDB, err := Open(dbOption)
	defer func() {
		_ = trainKVDB.Close()
	}()
	if err != nil {
		common.Panic(err)
	}

	// Get Put
	for i := 0; i < 60; i++ {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		e := model.Entry{Key: []byte(key), Value: []byte(val)}
		e.WithTTL(1000 * time.Second)
		if err = trainKVDB.Set(&e); err != nil {
			t.Fatal(err)
		}
		if entry, err := trainKVDB.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}

	// Del
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("key%d", i)
		if err = trainKVDB.Del([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	// Iterator
	dbIterator := trainKVDB.NewDBIterator(&model.Options{Prefix: []byte("hello"), IsAsc: true})
	defer dbIterator.Close()
	for dbIterator.Rewind(); dbIterator.Valid(); dbIterator.Next() {
		item := dbIterator.Item().Item
		t.Logf("dbIterator.Item key=%s, value=%s, expiresAt=%d",
			item.Key, item.Value, item.ExpiresAt)
	}
	fmt.Printf("dbIterator.Item over!\n")
}
