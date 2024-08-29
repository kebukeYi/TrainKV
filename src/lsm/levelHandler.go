package lsm

import (
	"bytes"
	"sort"
	"sync"
	"trainKv/common"
	"trainKv/model"
)

type levelHandler struct {
	mux            sync.RWMutex
	levelID        int
	tables         []*table
	totalSize      int64
	totalStaleSize int64         // 失效数据量
	lm             *levelsManger // 上层引用
}

func (leh *levelHandler) add(r *table) {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	leh.tables = append(leh.tables, r)
}

func (leh *levelHandler) addBatch(ts []*table) {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	leh.tables = append(leh.tables, ts...)
}

func (leh *levelHandler) addSize(t *table) {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	leh.totalSize += t.Size()
	leh.totalStaleSize += int64(t.getStaleDataSize())
}

func (leh *levelHandler) getTotalSize() int64 {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	return leh.totalSize
}

func (leh *levelHandler) subtractSize(t *table) {
	leh.totalSize -= t.Size()
	leh.totalStaleSize -= int64(t.getStaleDataSize())
}

func (leh *levelHandler) numTables() int {
	leh.mux.RLock()
	defer leh.mux.RUnlock()
	return len(leh.tables)
}

func (leh *levelHandler) Get(key []byte) (*model.Entry, error) {
	// 如果是第0层文件则进行特殊处理
	if leh.levelID == 0 {
		// 获取可能存在key的sst
		return leh.searchL0SST(key)
	} else {
		return leh.searchLnSST(key)
	}
}

func (leh *levelHandler) searchL0SST(key []byte) (*model.Entry, error) {
	var version uint64
	for i := len(leh.tables) - 1; i >= 0; i-- {
		table := leh.tables[i]
		if entry, err := table.Search(key, &version); err == nil {
			//fmt.Printf("level[%d] orginKey:%s | Meta: %v table:%s;\n", 0, model.ParseKey(key), entry.Meta, table.Name)
			return entry, nil
		}
	}
	return nil, common.ErrKeyNotFound
}

func (leh *levelHandler) searchLnSST(key []byte) (*model.Entry, error) {
	getTable := leh.getTable(key)
	if getTable == nil {
		return nil, common.ErrNotFoundTable
	}
	var version uint64
	var err error
	if entry, err := getTable.Search(key, &version); err == nil {
		//fmt.Printf("level[%d] orginKey:%s | Meta: %v table:%s;\n", leh.levelID, model.ParseKey(key), entry.Meta, getTable.Name)
		return entry, nil
	}
	common.Err(err)
	return nil, common.ErrKeyNotFound
}

// 默认从 首部 开始查询, 找到第一个大于等于key的sst, 除了 0层之外 ,其他层的 table 都是递增规律;
func (leh *levelHandler) getTable(key []byte) *table {
	if leh.numTables() > 0 && (bytes.Compare(leh.tables[0].sst.minKey, key) > 0 ||
		bytes.Compare(leh.tables[leh.numTables()-1].sst.maxKey, key) < 0) {
		return nil
	} else {
		for i := leh.numTables() - 1; i >= 0; i-- {
			if bytes.Compare(key, leh.tables[i].sst.MinKey()) > -1 ||
				bytes.Compare(key, leh.tables[i].sst.MaxKey()) < 1 {
				return leh.tables[i]
			}
		}
	}
	return nil
}

func (leh *levelHandler) isLastLevel() bool {
	return leh.levelID == leh.lm.lsm.option.MaxLevelNum-1
}

func (leh *levelHandler) Sort() {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	//if leh.numTables() == 0 { // 1. 逻辑错误(应该判断是否0层) 2. 理论错误(产生死锁)
	if leh.levelID == 0 {
		sort.Slice(leh.tables, func(i, j int) bool {
			return leh.tables[i].fid < leh.tables[j].fid
		})
	} else {
		sort.Slice(leh.tables, func(i, j int) bool {
			return model.CompareKey(leh.tables[i].sst.MinKey(), leh.tables[j].sst.MinKey()) < 0
		})
	}
}

type levelHandlerRLocked struct{}

func (leh *levelHandler) findOverLappingTables(_ levelHandlerRLocked, kr keyRange) (lIndex int, rIndex int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(leh.numTables(), func(i int) bool {
		return model.CompareKey(kr.left, leh.tables[i].sst.MinKey()) >= 0 &&
			model.CompareKey(kr.left, leh.tables[i].sst.MaxKey()) <= 0
	})
	if left == leh.numTables() {
		return 0, -1
	}
	right := sort.Search(leh.numTables(), func(i int) bool {
		return model.CompareKey(kr.right, leh.tables[i].sst.MinKey()) >= 0 &&
			model.CompareKey(kr.right, leh.tables[i].sst.MaxKey()) <= 0
	})
	if right == leh.numTables() {
		right = left
	}
	return left, right
}

func (leh *levelHandler) updateTable(toDel, toAdd []*table) error {
	leh.mux.Lock()
	toDelMap := make(map[uint64]bool, len(toDel))
	for _, t := range toDel {
		toDelMap[t.fid] = true
	}
	newTables := make([]*table, 0)
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
		return model.CompareKey(leh.tables[i].sst.MinKey(), leh.tables[j].sst.MinKey()) < 0
	})
	leh.mux.Unlock()
	return decrRefs(toDel)
}

func (leh *levelHandler) deleteTable(toDel []*table) error {
	leh.mux.Lock()
	toDelMap := make(map[uint64]bool, len(toDel))
	for _, t := range toDel {
		toDelMap[t.fid] = true
	}
	newTables := make([]*table, 0)
	for _, t := range leh.tables {
		if _, ok := toDelMap[t.fid]; ok {
			leh.subtractSize(t)
		} else {
			newTables = append(newTables, t)
		}
	}
	leh.tables = newTables
	sort.Slice(leh.tables, func(i, j int) bool {
		return model.CompareKey(leh.tables[i].sst.MinKey(), leh.tables[j].sst.MinKey()) < 0
	})
	leh.mux.Unlock()
	return decrRefs(toDel)
}

func (leh *levelHandler) iterators(opt *model.Options) []model.Iterator {
	leh.mux.Lock()
	defer leh.mux.Unlock()
	if leh.levelID == 0 {
		return iteratorsReversed(leh.tables, opt)
	}
	if len(leh.tables) == 0 {
		return nil
	}
	return []model.Iterator{NewConcatIterator(leh.tables, opt)}
}

func (leh *levelHandler) close() error {
	for i := range leh.tables {
		if err := leh.tables[i].sst.Close(); err != nil {
			return err
		}
	}
	return nil
}
