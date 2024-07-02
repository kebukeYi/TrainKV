package lsm

import (
	"time"
	"trainKv/common"
	"trainKv/model"
	"trainKv/utils"
)

type Options struct {
	WorkDir      string // 工作数据目录
	MemTableSize int64  // 内存表最大限制
	SSTableMaxSz int64  // SSSTable 最大限制
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int // 数据持久化时的大小
	// BloomFalsePositive is the false positive probability of bloom filter.
	BloomFalsePositive float64 // 布隆过滤器的容错率

	// compact 合并相关
	NumCompactors       int   // 合并协程数量;默认1
	BaseLevelSize       int64 // 基层中 所期望的文件大小
	LevelSizeMultiplier int   // 决定 level 之间期望 总体文件 size 比例, 比如是 10倍
	TableSizeMultiplier int   // 决定每层 文件 递增倍数
	BaseTableSize       int64 // 基层中 文件所期望的文件大小
	NumLevelZeroTables  int   // 第 0 层中,允许的表数量
	MaxLevelNum         int   // 最大层数,默认是 15层

	DiscardStatsCh *chan map[uint32]int64 //  用于 compact 组件 向 vlog 组件 传递信息使用,在合并过程中,知道哪些文件是失效的,让vlog组件知道,方便其GC;
}

type LSM struct {
	memoryTable   *memoryTable
	imemoryTables []*memoryTable
	levelManger   *levelsManger
	option        *Options
	maxMemFID     uint32
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{
		option: opt,
	}
	lsm.levelManger = lsm.InitLevelManger(opt)
	lsm.memoryTable, lsm.imemoryTables = lsm.recovery()
	//go lsm.flushTask()
	return lsm
}

func (lsm *LSM) Put(entry *model.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return common.ErrEmptyKey
	}
	if int64(lsm.memoryTable.wal.size)+int64(EstimateWalEncodeSize(entry)) >
		lsm.option.MemTableSize {
		lsm.Rotate()
	}

	err = lsm.memoryTable.Put(entry)
	if err != nil {
		return err
	}
	return nil
}

func (lsm *LSM) Get(key []byte) (*model.Entry, error) {
	if len(key) == 0 {
		return nil, common.ErrEmptyKey
	}
	entry, err := lsm.memoryTable.Get(key)
	common.PrintErr(err, "lsm.memoryTable.Get()")
	if entry != nil {
		return entry, nil
	}

	for i := len(lsm.imemoryTables) - 1; i >= 0; i-- {
		if entry, err = lsm.imemoryTables[i].Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}

	return lsm.levelManger.Get(key)
}

func (lsm *LSM) MemSize() int64 {
	return lsm.memoryTable.Size()
}

func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memoryTable == nil
}

func (lsm *LSM) GetSkipListFromMemTable() *utils.SkipList {
	return lsm.memoryTable.skipList
}

func (lsm *LSM) Rotate() {
	lsm.imemoryTables = append(lsm.imemoryTables, lsm.memoryTable)
	lsm.memoryTable = lsm.NewMemoryTable()
}

func (lsm *LSM) flushTask() {
	time.Sleep(time.Second * 10)
	// 检查是否存在immutable需要刷盘;
	for _, immutable := range lsm.imemoryTables {
		if err := lsm.levelManger.flush(immutable); err != nil {
			common.Err(err)
		}
		err := immutable.close()
		common.Err(err)
	}
}
