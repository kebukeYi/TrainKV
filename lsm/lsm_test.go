package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

var lsmTestPath = "/usr/golanddata/trainkv/lsm"

func TestLSM_Get(t *testing.T) {
	clearDir(lsmTestPath)
	opt := GetDefaultOpt(lsmTestPath)
	lsm := NewLSM(opt, utils.NewCloser(1))
	defer lsm.Close()
	key := []byte("testKey")
	value := []byte("testValue")

	// Test empty key
	_, err := lsm.Get([]byte{})
	assert.Equal(t, common.ErrEmptyKey, err)

	// Test key not found
	_, err = lsm.Get(key)
	assert.Equal(t, common.ErrEmptyKey, err)

	// Test key found in memoryTable
	keyWithTs := model.KeyWithTs(key, 1)
	newEntry := model.NewEntry(keyWithTs, value)
	lsm.memoryTable.Put(newEntry)
	entry, err := lsm.Get(keyWithTs)
	fmt.Printf("key:%s, val:%s\n", entry.Key, entry.Value)

	// Test key found in imemoryTables
	lsm.immemoryTables = append(lsm.immemoryTables, lsm.NewMemoryTable())
	keyWithTs = model.KeyWithTs([]byte("newKey"), 1)
	e := model.NewEntry(keyWithTs, value)
	lsm.immemoryTables[0].Put(e)
	entry, err = lsm.Get(keyWithTs)
	fmt.Printf("newKey:%s, val:%s\n", entry.Key, entry.Value)
}

func TestLSM_Put(t *testing.T) {
	clearDir(lsmTestPath)
	opt := GetDefaultOpt(lsmTestPath)
	lsm := NewLSM(opt, utils.NewCloser(1))
	defer lsm.Close()
	newEntry := model.NewEntry(model.KeyWithTs([]byte("testKey"), 1), []byte("testValue"))
	// Test successful Put
	success := lsm.Put(newEntry)
	common.Panic(success)

	newEntry.Value = []byte("testValue2")
	success = lsm.Put(newEntry) // 更新操作
	common.Panic(success)
}
