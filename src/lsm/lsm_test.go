package lsm

import (
	"fmt"
	"testing"
	errors "trainKv/common"
	"trainKv/model"
)

var lsmOptions = &Options{
	WorkDir:      "/usr/projects_gen_data/goprogendata/trainkvdata/test/lsm",
	MemTableSize: 1000,
}

func TestLSM_Get(t *testing.T) {
	lsm := NewLSM(lsmOptions)

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
	keyWithTs := model.KeyWithTs(key)
	lsm.memoryTable.Put(&model.Entry{Key: keyWithTs, Value: value})
	entry, err := lsm.Get(keyWithTs)
	errors.Panic(err)
	fmt.Printf("entry: %v\n", entry)
	//assert.NoError(t, err)
	//assert.Equal(t, &model.Entry{Key: keyWithTs, Value: value}, entry)

	// Test key found in imemoryTables
	lsm.immemoryTables = append(lsm.immemoryTables, lsm.NewMemoryTable())
	keyWithTs = model.KeyWithTs(key)
	lsm.immemoryTables[0].Put(&model.Entry{Key: keyWithTs, Value: value})
	entry, err = lsm.Get(keyWithTs)
	errors.Panic(err)
	fmt.Printf("entry: %v\n", entry)
	//assert.NoError(t, err)
	//assert.Equal(t, &model.Entry{Key: keyWithTs, Value: value}, entry)
}

func TestLSM_Put(t *testing.T) {
	lsm := NewLSM(lsmOptions)
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
