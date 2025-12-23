package lsm

import (
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/interfaces"
	"github.com/kebukeYi/TrainKV/model"
	"sort"
	"sync"
)

type levelHandler struct {
	mux            sync.RWMutex
	levelID        int
	tables         []*Table
	totalSize      int64
	totalStaleSize int64         // 失效数据量;
	lm             *LevelsManger // 上层引用;
}

func (leh *levelHandler) add(r *Table) {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	leh.tables = append(leh.tables, r)
}

func (leh *levelHandler) addSize(t *Table) {
	leh.totalSize += t.Size()
	leh.totalStaleSize += int64(t.getStaleDataSize())
}

func (leh *levelHandler) getTotalSize() int64 {
	leh.mux.RLock()
	defer leh.mux.RUnlock()
	return leh.totalSize
}

func (leh *levelHandler) subtractSize(t *Table) {
	leh.totalSize -= t.Size()
	leh.totalStaleSize -= int64(t.getStaleDataSize())
}

func (leh *levelHandler) numTables() int {
	leh.mux.RLock()
	defer leh.mux.RUnlock()
	return len(leh.tables)
}

func (leh *levelHandler) Get(keyTs []byte) (model.Entry, error) {
	// 如果是第0层查询,则需要全部table进行逆序查询;
	if leh.levelID == 0 {
		return leh.searchL0SST(keyTs)
	} else {
		return leh.searchLnSST(keyTs)
	}
}

func (leh *levelHandler) searchL0SST(keyTs []byte) (model.Entry, error) {
	var maxEntry model.Entry
	for i := len(leh.tables) - 1; i >= 0; i-- {
		table := leh.tables[i]
		// 结果集:
		// 1. 没有找到;
		// 2. 等于找到;
		// 3. 找到小于当前 ketTs的;
		entry, _ := table.Search(keyTs)
		if entry.Value != nil || entry.Version != 0 {
			if entry.Version > maxEntry.Version {
				maxEntry = entry
				continue
			}
		}
	}
	return maxEntry, nil
}

func (leh *levelHandler) searchLnSST(keyTs []byte) (model.Entry, error) {
	getTable := leh.getTable(keyTs)
	if getTable == nil {
		return model.Entry{Version: 0}, common.ErrNotFoundTable
	}
	var maxEntry model.Entry
	// 结果集:
	// 1. 没有找到;
	// 2. 等于找到;
	// 3. 找到小于当前 ketTs的;
	entry, _ := getTable.Search(keyTs)
	if entry.Value != nil || entry.Version != 0 {
		if entry.Version > maxEntry.Version {
			maxEntry = entry
		}
	}
	return maxEntry, nil
}

// 默认从 首部 开始查询, 找到第一个最大值 大于等于 key的 sst, 除了l0层之外, 其他层的 Table 都是递增规律;
func (leh *levelHandler) getTable(key []byte) *Table {
	idx := sort.Search(len(leh.tables), func(i int) bool {
		maxKey := leh.tables[i].sst.MaxKey()
		cmp := model.CompareKeyWithTs(maxKey, key)
		return cmp >= 0
	})
	if idx >= len(leh.tables) {
		return nil
	}
	tbl := leh.tables[idx]
	return tbl
}

func (leh *levelHandler) isLastLevel() bool {
	return leh.levelID == leh.lm.lsm.option.MaxLevelNum-1
}

func (leh *levelHandler) Sort() {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	if leh.levelID == 0 {
		sort.Slice(leh.tables, func(i, j int) bool {
			return leh.tables[i].fid < leh.tables[j].fid
		})
	} else {
		sort.Slice(leh.tables, func(i, j int) bool {
			return model.CompareKeyWithTs(leh.tables[i].sst.MinKey(), leh.tables[j].sst.MinKey()) < 0
		})
	}
}

type levelHandlerRLocked struct{}

// 在本层所有的 Table 中找到涉及到给定的 kr 区间的 right,left左右边界;
func (leh *levelHandler) findOverLappingTables(_ levelHandlerRLocked, kr keyRange) (lIndex int, rIndex int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(leh.tables), func(i int) bool {
		return model.CompareKeyWithTs(kr.left, leh.tables[i].sst.MaxKey()) <= 0
	})
	right := sort.Search(len(leh.tables), func(i int) bool {
		return model.CompareKeyWithTs(kr.right, leh.tables[i].sst.MinKey()) < 0
	})
	return left, right
}

func (leh *levelHandler) updateTable(toDel, toAdd []*Table) error {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	toDelMap := make(map[uint64]bool, len(toDel))
	for _, t := range toDel {
		toDelMap[t.fid] = true
	}
	newTables := make([]*Table, 0)
	for _, t := range leh.tables {
		if _, ok := toDelMap[t.fid]; ok {
			leh.subtractSize(t)
		} else {
			newTables = append(newTables, t)
		}
	}

	for _, t := range toAdd {
		leh.addSize(t)
		t.IncrRef()
		newTables = append(newTables, t)
	}

	leh.tables = newTables
	sort.Slice(leh.tables, func(i, j int) bool {
		return model.CompareKeyWithTs(leh.tables[i].sst.MinKey(), leh.tables[j].sst.MinKey()) < 0
	})

	return decrRefs(toDel)
}

func (leh *levelHandler) deleteTable(toDel []*Table) error {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	toDelMap := make(map[uint64]bool, len(toDel))
	for _, t := range toDel {
		toDelMap[t.fid] = true
	}
	newTables := make([]*Table, 0)
	for _, t := range leh.tables {
		if _, ok := toDelMap[t.fid]; ok {
			leh.subtractSize(t)
		} else {
			newTables = append(newTables, t)
		}
	}
	leh.tables = newTables
	sort.Slice(leh.tables, func(i, j int) bool {
		return model.CompareKeyWithTs(leh.tables[i].sst.MinKey(), leh.tables[j].sst.MinKey()) < 0
	})
	return decrRefs(toDel)
}

func (leh *levelHandler) iterators(opt *interfaces.Options) []interfaces.Iterator {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	if leh.levelID == 0 {
		return iteratorsReversed(leh.tables, opt)
	}
	if len(leh.tables) == 0 {
		return nil
	}
	return []interfaces.Iterator{NewConcatIterator(leh.tables, opt)}
}

func (leh *levelHandler) close() error {
	for i := range leh.tables {
		if err := leh.tables[i].sst.Close(); err != nil {
			return err
		}
	}
	return nil
}
