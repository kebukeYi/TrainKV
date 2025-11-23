package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

var lsmTestPath = "/usr/golanddata/trainkv/lsm"

var lsmOptions = &Options{
	WorkDir:             lsmTestPath,
	MemTableSize:        10 << 10, // 10KB; 默认:64 << 20(64MB)
	NumFlushMemtables:   1,        // 默认：15;
	BlockSize:           2 * 1024, // 默认:4 * 1024
	BloomFalsePositive:  0.01,     // 误差率
	CacheNums:           1 * 1024, // 默认:10240个
	ValueThreshold:      1,        // 1B; 默认:1 << 20(1MB)
	ValueLogMaxEntries:  100,      // 默认:1000000
	ValueLogFileSize:    1 << 29,  // 512MB; 默认:1<<30-1(1GB);
	VerifyValueChecksum: false,    // 默认:false

	MaxBatchCount: 100,
	MaxBatchSize:  10 << 20, // 10 << 20(10MB)

	NumCompactors:       2,                  // 默认:4
	BaseLevelSize:       8 << 20,            //8MB; 默认: 10 << 20(10MB)
	LevelSizeMultiplier: 10,                 // 默认:10
	TableSizeMultiplier: 2,                  // 默认:2
	BaseTableSize:       2 << 20,            // 2 << 20(2MB)
	NumLevelZeroTables:  5,                  // 默认:5
	MaxLevelNum:         common.MaxLevelNum, // 默认:7
}

func TestLSM_Get(t *testing.T) {
	clearDir(lsmOptions.WorkDir)
	lsm := NewLSM(lsmOptions, utils.NewCloser(1))
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
	keyWithTs := model.KeyWithTs(key)
	newEntry := model.NewEntry(keyWithTs, value)
	lsm.memoryTable.Put(newEntry)
	entry, err := lsm.Get(keyWithTs)
	fmt.Printf("key:%s, val:%s\n", entry.Key, entry.Value)

	// Test key found in imemoryTables
	lsm.immemoryTables = append(lsm.immemoryTables, lsm.NewMemoryTable())
	keyWithTs = model.KeyWithTs([]byte("newKey"))
	e := model.NewEntry(keyWithTs, value)
	lsm.immemoryTables[0].Put(e)
	entry, err = lsm.Get(keyWithTs)
	fmt.Printf("newKey:%s, val:%s\n", entry.Key, entry.Value)
}

func TestLSM_Put(t *testing.T) {
	clearDir(lsmOptions.WorkDir)
	lsm := NewLSM(lsmOptions, utils.NewCloser(1))
	defer lsm.Close()
	newEntry := model.NewEntry(model.KeyWithTs([]byte("testKey")), []byte("testValue"))
	// Test successful Put
	success := lsm.Put(newEntry)
	common.Panic(success)

	newEntry.Value = []byte("testValue2")
	success = lsm.Put(newEntry) // 更新操作
	common.Panic(success)
}
