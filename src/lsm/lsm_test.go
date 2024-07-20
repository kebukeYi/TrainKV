package lsm

import (
	"fmt"
	"testing"
	"time"
	errors "trainKv/common"
	"trainKv/model"
)

func TestLSM_Get(t *testing.T) {
	lsm := NewLSM(&Options{
		WorkDir:      "F:\\TrainKV\\wal",
		MemTableSize: 1000,
	})

	key := []byte("testKey")
	value := []byte("testValue")

	// Test empty key
	_, err := lsm.Get([]byte{})
	errors.Panic(err)
	//assert.Equal(t, errors.ErrEmptyKey, err)

	// Test key not found
	_, err = lsm.Get(key)
	errors.Panic(err)
	//assert.Equal(t, errors.ErrNotFound, err)

	// Test key found in memoryTable
	keyWithTs := model.KeyWithTs(key, uint64(time.Now().Unix()/1e9))
	lsm.memoryTable.Put(&model.Entry{Key: keyWithTs, Value: value})
	entry, err := lsm.Get(keyWithTs)
	errors.Panic(err)
	fmt.Printf("entry: %v\n", entry)
	//assert.NoError(t, err)
	//assert.Equal(t, &model.Entry{Key: keyWithTs, Value: value}, entry)

	// Test key found in imemoryTables
	lsm.imemoryTables = append(lsm.imemoryTables, lsm.NewMemoryTable())
	keyWithTs = model.KeyWithTs(key, uint64(time.Now().Unix()/1e9))
	lsm.imemoryTables[0].Put(&model.Entry{Key: keyWithTs, Value: value})
	entry, err = lsm.Get(keyWithTs)
	errors.Panic(err)
	fmt.Printf("entry: %v\n", entry)
	//assert.NoError(t, err)
	//assert.Equal(t, &model.Entry{Key: keyWithTs, Value: value}, entry)
}

func TestLSM_Put(t *testing.T) {
	lsm := NewLSM(&Options{
		WorkDir:      "F:\\TrainKV\\wal",
		MemTableSize: 1000,
	})
	entry := &model.Entry{Key: []byte("testKey"), Value: []byte("testValue")}

	// Test successful Put
	success := lsm.Put(entry)
	errors.Panic(success)
	//assert.True(t, success)

	// Test Put failure due to error in memoryTable Put.
	success = lsm.Put(entry) // 更新操作
	errors.Panic(success)
	//assert.True(t, success)
}
