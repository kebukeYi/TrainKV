package lsm

import (
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/skl"
	"github.com/kebukeYi/TrainKV/utils"
	"sync"
)

type LSM struct {
	sync.RWMutex
	memoryTable    *memoryTable
	immemoryTables []*memoryTable
	LevelManger    *LevelsManger
	option         *Options
	maxMemFID      uint32

	flushMemTable chan *memoryTable

	ExpiredValPtrChan chan model.ValuePtr // compact`MergeIterator`fix() to lsm;
	ExpiredValNum     int
	ExpiredValSize    int64
}

func NewLSM(opt *Options, closer *utils.Closer) *LSM {
	lsm := &LSM{
		option:        opt,
		flushMemTable: make(chan *memoryTable, opt.NumFlushMemtables),
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

	if int64(lsm.memoryTable.wal.Size())+int64(EstimateWalEncodeSize(entry)) > lsm.option.MemTableSize {
		//fmt.Printf("memtable is full, rotate memtable when cur entry key:%s | meta:%d | value: %s ;\n", model.ParseKey(entry.Key), entry.Meta, entry.Value)
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

func (lsm *LSM) Get(keyTs []byte) (model.Entry, error) {
	if len(keyTs) <= 8 {
		return model.Entry{Version: -1}, common.ErrEmptyKey
	}
	var (
		entry model.Entry
		err   error
	)
	// 1. 跳表中进行对比, key 去除掉 Ts时间戳进行对比,相同直接返回到lsm,将不再继续向level层寻找;
	entry, err = lsm.memoryTable.Get(keyTs)
	if entry.Version != -1 {
		return entry, err
	}
	for i := len(lsm.immemoryTables) - 1; i >= 0; i-- {
		if entry, err = lsm.immemoryTables[i].Get(keyTs); entry.Version != -1 {
			return entry, err
		}
	}
	return lsm.LevelManger.Get(keyTs)
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
	lsm.flushMemTable <- im
}

func (lsm *LSM) StartFlushMemTable(closer *utils.Closer) {
	defer closer.Done()
	flushIMemoryTable := func(im *memoryTable) {
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
	n := lsm.option.NumCompactors
	closer.Add(n - 1)
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
