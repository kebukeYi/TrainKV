package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

var lsmTestPath = "/usr/projects_gen_data/goprogendata/trainkvdata/test/lsm"

var lsmOptions = &Options{
	WorkDir:             lsmTestPath,
	MemTableSize:        1 << 10,  // 1KB; 默认:64 << 20(64MB)
	NumFlushMemtables:   1,        // 默认：15;
	SSTableMaxSz:        1 << 10,  // 同上
	BlockSize:           3 * 1024, // 默认:4 * 1024
	BloomFalsePositive:  0.01,     // 误差率
	CacheNums:           1 * 1024, // 默认:10240个
	ValueThreshold:      1,        // 1B; 默认:1 << 20(1MB)
	ValueLogMaxEntries:  100,      // 默认:1000000
	ValueLogFileSize:    1 << 29,  // 512MB; 默认:1<<30-1(1GB);
	VerifyValueChecksum: false,    // 默认:false

	MaxBatchCount: 10,      // 实时计算
	MaxBatchSize:  1 << 20, // 实时计算

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
	lsm.memoryTable.Put(model.Entry{Key: keyWithTs, Value: value})
	entry, err := lsm.Get(keyWithTs)
	fmt.Printf("key:%s, val:%s\n", entry.Key, entry.Value)

	// Test key found in imemoryTables
	lsm.immemoryTables = append(lsm.immemoryTables, lsm.NewMemoryTable())
	keyWithTs = model.KeyWithTs([]byte("newKey"))
	lsm.immemoryTables[0].Put(model.Entry{Key: keyWithTs, Value: value})
	entry, err = lsm.Get(keyWithTs)
	fmt.Printf("newKey:%s, val:%s\n", entry.Key, entry.Value)
}

func TestLSM_Put(t *testing.T) {
	clearDir(lsmOptions.WorkDir)
	lsm := NewLSM(lsmOptions, utils.NewCloser(1))
	defer lsm.Close()
	entry := model.Entry{Key: model.KeyWithTs([]byte("testKey")), Value: []byte("testValue")}

	// Test successful Put
	success := lsm.Put(entry)
	common.Panic(success)

	// Test Put failure due to error in memoryTable Put.
	success = lsm.Put(entry) // 更新操作
	common.Panic(success)

}
