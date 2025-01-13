package lsm

import (
	"fmt"
	"trainKv/common"
	"trainKv/model"
	"trainKv/utils"
)

type Options struct {
	WorkDir      string // 工作数据目录
	MemTableSize int64  // 内存表最大限制
	SSTableMaxSz int64  // SSSTable 最大限制
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize uint32 // 数据持久化时的大小
	// BloomFalsePositive is the false positive probability of bloom filter.
	BloomFalsePositive float64 // 布隆过滤器的容错率

	CacheSize int // 缓存容量 B

	// compact 合并相关
	NumCompactors       int   // 合并协程数量;默认1
	BaseLevelSize       int64 // 基层中 所期望的文件大小
	LevelSizeMultiplier int   // 决定 level 之间期望 总体文件 size 比例, 比如是 10倍
	TableSizeMultiplier int   // 决定每层 文件 递增倍数
	BaseTableSize       int64 // 基层中 文件所期望的文件大小
	NumLevelZeroTables  int   // 第 0 层中,允许的表数量
	MaxLevelNum         int   // 最大层数,默认是 15层

	ExpiredValPtrChan chan model.ValuePtr // compact to lsm
	ExpiredValNum     int
	ExpiredValSize    int64

	DiscardStatsCh *chan map[uint32]int64 //  用于 compact 组件 向 vlog 组件 传递信息使用,在合并过程中,知道哪些文件是失效的,让vlog组件知道,方便其GC;
}

type LSM struct {
	memoryTable    *memoryTable
	immemoryTables []*memoryTable
	levelManger    *levelsManger
	option         *Options
	maxMemFID      uint32
	Closer         *utils.Closer
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{
		option: opt,
	}
	lsm.levelManger = lsm.InitLevelManger(opt)
	lsm.memoryTable, lsm.immemoryTables = lsm.recovery()
	lsm.Closer = utils.NewCloser()
	return lsm
}

func (lsm *LSM) Put(entry *model.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return common.ErrEmptyKey
	}
	if int64(lsm.memoryTable.wal.Size())+int64(EstimateWalEncodeSize(entry)) >
		lsm.option.MemTableSize {
		fmt.Printf("memtable is full, rotate memtable when entry key:%s | value: %s\n",
			entry.Key, entry.Value)
		lsm.Rotate()
	}

	// 1. 跳表中进行对比时, key 去除掉 Ts 版本号
	// 2. 添加进跳表中的 key 是携带有 Ts版本号
	// 3. wal的 key 是携带有 Ts版本号
	err = lsm.memoryTable.Put(entry)
	if err != nil {
		return err
	}

	// 检查是否存在immutable需要刷盘，
	for _, immutable := range lsm.immemoryTables {
		if err = lsm.levelManger.flush(immutable); err != nil {
			return err
		}
		fmt.Printf("flush immutable table currKey:%s | Meta: %v\n",
			immutable.currKey, immutable.currKeyMeta)
		// TODO 这里问题很大，应该是用引用计数的方式回收
		err = immutable.close(true)
		common.Panic(err)
	}
	if len(lsm.immemoryTables) != 0 {
		// TODO 将lsm的immutables队列置空，这里可以优化一下节省内存空间，还可以限制一下immut table的大小为固定值
		lsm.immemoryTables = make([]*memoryTable, 0)
	}
	return err
}

func (lsm *LSM) Get(key []byte) (*model.Entry, error) {
	if len(key) == 0 {
		return nil, common.ErrEmptyKey
	}
	// 1. 跳表中进行对比时, key 去除掉 Ts 版本号
	entry, err := lsm.memoryTable.Get(key)
	if entry != nil && entry.Value != nil {
		//fmt.Printf("memtable[%d] orginKey:%s | Meta: %v | v:%s;\n", 0, model.ParseKey(key), entry.Meta, entry.Value)
		return entry, nil
	}

	for i := len(lsm.immemoryTables) - 1; i >= 0; i-- {
		if entry, err = lsm.immemoryTables[i].Get(key); entry != nil && entry.Value != nil {
			//fmt.Printf("immutables[%d] orginKey:%s | Meta: %v | v:%s;\n", 0,
			//	model.ParseKey(key), entry.Meta, entry.Value)
			return entry, err
		}
	}
	// 从level manger查询
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
	lsm.immemoryTables = append(lsm.immemoryTables, lsm.memoryTable)
	lsm.memoryTable = lsm.NewMemoryTable()
}
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	lsm.Closer.Add(n)
	for i := 0; i < n; i++ {
		go lsm.levelManger.runCompacter(i)
	}
}
func (lsm *LSM) Close() error {
	lsm.Closer.Close()
	if lsm.memoryTable != nil {
		if err := lsm.memoryTable.close(false); err != nil {
			return err
		}
	}
	for i := range lsm.immemoryTables {
		if err := lsm.immemoryTables[i].close(false); err != nil {
			return err
		}
	}
	if err := lsm.levelManger.close(); err != nil {
		return err
	}
	return nil
}
