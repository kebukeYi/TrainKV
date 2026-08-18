package lsm

import (
	"sort"
	"sync"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/model"
)

type LevelHandler struct {
	mux            sync.RWMutex
	levelID        int
	tables         []*Table
	totalSize      int64
	totalStaleSize int64         // 失效数据量;
	lm             *LevelsManger // 上层引用;
}

func (leh *LevelHandler) add(r *Table) {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	leh.tables = append(leh.tables, r)
}

func (leh *LevelHandler) addSize(t *Table) {
	leh.totalSize += t.Size()
	leh.totalStaleSize += int64(t.getStaleDataSize())
}

func (leh *LevelHandler) getTotalSize() int64 {
	leh.mux.RLock()
	defer leh.mux.RUnlock()
	return leh.totalSize
}

func (leh *LevelHandler) subtractSize(t *Table) {
	leh.totalSize -= t.Size()
	leh.totalStaleSize -= int64(t.getStaleDataSize())
}

func (leh *LevelHandler) numTables() int {
	leh.mux.RLock()
	defer leh.mux.RUnlock()
	return len(leh.tables)
}

func (leh *LevelHandler) Get(keyTs []byte) (model.Entry, error) {
	// 如果是第0层查询,则需要全部table进行逆序查询;
	if leh.levelID == 0 {
		return leh.searchL0SST(keyTs)
	}
	return leh.searchLnSST(keyTs)
}

func (leh *LevelHandler) searchL0SST(keyTs []byte) (model.Entry, error) {
	// [old,1,2,3,4,5,6,7,8,new...]
	leh.mux.RLock()
	tables := make([]*Table, len(leh.tables))
	copy(tables, leh.tables)
	for _, t := range tables {
		// 要在加锁状态下,提前将所有的 table 引用起来, 避免后被 compact 协程删除掉;
		t.IncrRef()
	}
	leh.mux.RUnlock()
	defer func() {
		for _, t := range tables {
			_ = t.DecrRef()
		}
	}()
	var maxEntry model.Entry
	for i := len(tables) - 1; i >= 0; i-- {
		table := tables[i]
		// 多种结果集:
		// 1. 没有找到;
		// 2. 等于找到;
		// 3. 找到最近小于当前 keyTs 的;
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

func (leh *LevelHandler) searchLnSST(keyTs []byte) (model.Entry, error) {
	leh.mux.RLock()
	tbl := leh.getTable(keyTs) // getTable 仅允许在 RLock 下调用;
	if tbl == nil {
		leh.mux.RUnlock()
		return model.Entry{Version: 0}, common.ErrNotFoundTable
	}
	tbl.IncrRef() // 必须在锁内加引用,防止 RUnlock 后被 decrRef 到 0 删除
	leh.mux.RUnlock()
	defer func() { _ = tbl.DecrRef() }()

	var maxEntry model.Entry
	// 结果集:
	// 1. 没有找到;
	// 2. 等于找到;
	// 3. 找到小于当前 keyTs 的;
	entry, _ := tbl.Search(keyTs)
	if entry.Value != nil || entry.Version != 0 {
		if entry.Version > maxEntry.Version {
			maxEntry = entry
		}
	}
	return maxEntry, nil
}

// 默认从 首部 开始查询, 找到第一个最大值 大于等于 key的 sst, 除了l0层之外, 其他层的 Table 都是递增规律;
func (leh *LevelHandler) getTable(key []byte) *Table {
	// 手写二分, 避免 sort.Search 闭包在每次 Get 时分配;
	lo, hi := 0, len(leh.tables)
	for lo < hi {
		mid := (lo + hi) / 2
		if model.CompareKeyWithTs(leh.tables[mid].sst.MaxKey(), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= len(leh.tables) {
		return nil
	}
	tbl := leh.tables[lo]
	return tbl
}

func (leh *LevelHandler) isLastLevel() bool {
	return leh.levelID == leh.lm.lsm.Option.MaxLevelNum-1
}

func (leh *LevelHandler) Sort() {
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
func (leh *LevelHandler) findOverLappingTables(_ levelHandlerRLocked, kr keyRange) (lIndex int, rIndex int) {
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

func (leh *LevelHandler) updateTable(toDel, toAdd []*Table) error {
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

func (leh *LevelHandler) deleteTable(toDel []*Table) error {
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

func (leh *LevelHandler) iterators(opt *interfaces.Options) []interfaces.Iterator {
	leh.mux.RLock()
	defer leh.mux.RUnlock()
	if leh.levelID == 0 {
		return iteratorsReversed(leh.tables, opt)
	}
	if len(leh.tables) == 0 {
		return nil
	}
	return []interfaces.Iterator{NewConcatIterator(leh.tables, opt)}
}

func (leh *LevelHandler) close() error {
	for i := range leh.tables {
		if err := leh.tables[i].sst.Close(); err != nil {
			return err
		}
	}
	return nil
}
