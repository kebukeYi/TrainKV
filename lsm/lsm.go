package lsm

import (
	"sync"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/skl"
	"github.com/kebukeYi/TrainKV/v2/utils"
)

type LSM struct {
	sync.RWMutex
	memoryTable    *MemoryTable
	immemoryTables []*MemoryTable
	LevelManger    *LevelsManger
	Option         *Options

	flushMemTable chan *MemoryTable

	ExpiredValPtrChan chan model.ValuePtr // compact`MergeIterator`fix() to lsm;
	ExpiredValNum     int
	ExpiredValSize    int64
}

func NewLSM(opt *Options, closer *utils.Closer) *LSM {
	lsm := &LSM{
		Option:        opt,
		flushMemTable: make(chan *MemoryTable, opt.WaitFlushMemTables),
	}
	// 1. 更新 lm.maxID
	lsm.LevelManger = lsm.InitLevelManger(opt)

	go lsm.StartFlushMemTable(closer) // lock

	// 2. 更新 lm.maxID
	lsm.memoryTable, lsm.immemoryTables = lsm.recovery()
	for _, im := range lsm.immemoryTables {
		lsm.flushMemTable <- im
	}
	return lsm
}

func (lsm *LSM) Put(entry *model.Entry) (err error) {
	if entry.Key == nil || len(entry.Key) == 0 || len(entry.Key) <= 8 {
		return common.ErrEmptyKey
	}

	if int64(lsm.memoryTable.wal.Size())+int64(EstimateWalEncodeSize(entry)) > lsm.Option.MemTableSize {
		lsm.Rotate()
	}

	// 1. 跳表中进行对比时, key 去除掉 Ts 时间戳号, 原生key相同则更新;
	// 2. 添加进跳表中的 key 是携带有 Ts时间戳;
	// 3. wal文件持久化的的 key 是携带有 Ts时间错;
	err = lsm.memoryTable.Put(entry)
	if err != nil {
		return err
	}

	return err
}

func (lsm *LSM) SyncWalFile() error {
	if err := lsm.memoryTable.SyncWalFile(); err != nil {
		return err
	}
	return nil
}

func (lsm *LSM) Get(keyTs []byte) (model.Entry, error) {
	if len(keyTs) <= 8 {
		return model.Entry{Version: 0}, common.ErrEmptyKey
	}
	var (
		maxEntryTs model.Entry
	)
	startTs := model.ParseTsVersion(keyTs)

	// Read of IncrRef; 无 immutable 时直接在栈上持有引用, 避免每次 Get 分配切片;
	lsm.RLock()
	mt := lsm.memoryTable
	mt.IncrRef()
	var imms []*MemoryTable
	if n := len(lsm.immemoryTables); n > 0 {
		imms = make([]*MemoryTable, n)
		for i := 0; i < n; i++ {
			im := lsm.immemoryTables[n-1-i] // 新→旧, 与 getAllMemoryTales 顺序一致;
			im.IncrRef()
			imms[i] = im
		}
	}
	lsm.RUnlock()
	defer func() { // DecrRef;
		mt.DecrRef()
		for _, im := range imms {
			im.DecrRef()
		}
	}()

	entry, _ := mt.Get(keyTs)
	if entry.Version != 0 || entry.Value != nil {
		// 1. 跳表中对返回的near节点进行对比时, key 是去掉Ts时间戳的, 相同直接返回,将不再继续向level层寻找;
		// 否则向level--层寻找;
		if entry.Version == startTs {
			return entry, nil
		}
		if entry.Version > maxEntryTs.Version {
			maxEntryTs = entry
		}
	}
	for _, memoryTable := range imms {
		entry, _ := memoryTable.Get(keyTs)
		if entry.Version == 0 && entry.Value == nil {
			continue
		}
		if entry.Version == startTs {
			return entry, nil
		}
		if entry.Version > maxEntryTs.Version {
			maxEntryTs = entry
		}
	} // skipList over
	// 2. level 0-7 层 进行寻找;
	return lsm.LevelManger.Get(keyTs, &maxEntryTs)
}

func (lsm *LSM) MaxVersion() uint64 {
	lsm.RLock()
	defer lsm.RUnlock()
	var maxVersion uint64
	maxVersion = lsm.memoryTable.maxVersion
	for _, table := range lsm.immemoryTables {
		if table.maxVersion > maxVersion {
			maxVersion = table.maxVersion
		}
	}

	for i := 0; i < common.MaxLevelNum; i++ {
		tables := lsm.LevelManger.levelHandlers[i].tables
		for _, table := range tables {
			if table.MaxVersion() > maxVersion {
				maxVersion = table.MaxVersion()
			}
		}
	}

	return maxVersion
}

func (lsm *LSM) MemSize() int64 {
	return lsm.memoryTable.Size()
}

func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memoryTable == nil
}

func (lsm *LSM) GetSkipListFromMemTable() *skl.SkipList {
	return lsm.memoryTable.skipList
}

func (lsm *LSM) Rotate() {
	lsm.Lock()
	im := lsm.memoryTable
	lsm.immemoryTables = append(lsm.immemoryTables, lsm.memoryTable)
	lsm.memoryTable = lsm.NewMemoryTable()
	lsm.Unlock()
	// 通道有可能阻塞;
	lsm.flushMemTable <- im
}

func (lsm *LSM) StartFlushMemTable(closer *utils.Closer) {
	defer closer.Done()
	flushIMemoryTable := func(im *MemoryTable) {
		if im == nil {
			return
		}
		if err := lsm.LevelManger.flush(im); err != nil {
			common.Panic(err)
		}
		lsm.Lock()
		lsm.immemoryTables = lsm.immemoryTables[1:]
		im.skipList.DecrRef()
		lsm.Unlock()
	}

	for {
		select {
		case im := <-lsm.flushMemTable:
			flushIMemoryTable(im)
		case <-closer.CloseSignal:
			for im := range lsm.flushMemTable {
				flushIMemoryTable(im)
			}
			return
		}
	}
}

func (lsm *LSM) CloseFlushIMemChan() {
	close(lsm.flushMemTable)
}

func (lsm *LSM) StartCompacter(closer *utils.Closer) {
	n := lsm.Option.NumCompactors
	for coroutineID := 0; coroutineID < n; coroutineID++ {
		go lsm.LevelManger.runCompacter(coroutineID, closer)
	}
}

func (lsm *LSM) Close() error {
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
	if err := lsm.LevelManger.close(); err != nil {
		return err
	}
	return nil
}
