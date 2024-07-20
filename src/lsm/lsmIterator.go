package lsm

import (
	"sort"
	"trainKv/common"
	"trainKv/interfaces"
	"trainKv/model"
)

type lsmIterator struct {
	iters []interfaces.Iterator
	item  interfaces.Item
}

func (lsm *LSM) NewLsmIterator() []interfaces.Iterator {
	iter := &lsmIterator{}
	iter.iters = make([]interfaces.Iterator, 0)
	iter.iters = append(iter.iters, lsm.memoryTable.skipList.NewSkipListIterator())
	for _, imemoryTable := range lsm.imemoryTables {
		iter.iters = append(iter.iters, imemoryTable.skipList.NewSkipListIterator())
	}
	iter.iters = append(iter.iters, lsm.levelManger.iterators()...)
	return iter.iters
}

func (lsmIter *lsmIterator) Next() {
	lsmIter.iters[0].Next()
}

func (lsmIter *lsmIterator) Valid() bool {
	return lsmIter.iters[0].Valid()
}

func (lsmIter *lsmIterator) Rewind() {
	lsmIter.iters[0].Rewind()
}

func (lsmIter *lsmIterator) Seek(key []byte) {

}
func (lsmIter *lsmIterator) Item() interfaces.Item {
	return lsmIter.iters[0].Item()
}
func (lsmIter *lsmIterator) Close() error {
	return nil
}

type ConcatIterator struct {
	tables []*table
	iters  []interfaces.Iterator
	idx    int
	curIer interfaces.Iterator
	opt    *interfaces.Options
}

func NewConcatIterator(tables []*table, opt *interfaces.Options) *ConcatIterator {
	return &ConcatIterator{
		tables: tables,
		iters:  make([]interfaces.Iterator, len(tables)),
		idx:    -1,
		curIer: nil,
		opt:    opt,
	}
}

func (conIter *ConcatIterator) setIdx(inx int) {
	if inx < 0 || inx >= len(conIter.tables) {
		conIter.curIer = nil
		return
	}
	conIter.idx = inx
	if conIter.iters[inx] == nil {
		conIter.iters[inx] = conIter.tables[inx].NewTableIterator(conIter.opt)
	}
	conIter.curIer = conIter.iters[inx]
}

func (conIter *ConcatIterator) Rewind() {
	if len(conIter.iters) == 0 {
		return
	}
	if conIter.opt.IsAsc {
		// 升序: 从末尾开始遍历
		conIter.setIdx(len(conIter.iters) - 1)
	} else {
		conIter.setIdx(0)
	}
	conIter.curIer.Rewind()
}

func (conIter *ConcatIterator) Valid() bool {
	return conIter.curIer != nil && conIter.curIer.Valid()
}

func (conIter *ConcatIterator) Item() interfaces.Item {
	return conIter.curIer.Item()
}

func (conIter *ConcatIterator) Seek(key []byte) {
	var idx int
	if conIter.opt.IsAsc { // 升序遍历
		idx = sort.Search(len(conIter.tables), func(i int) bool {
			return model.CompareKey(conIter.tables[i].sst.MaxKey(), key) >= 0
		})
	} else { // 降序遍历
		idx = sort.Search(len(conIter.tables), func(i int) bool {
			return model.CompareKey(key, conIter.tables[i].sst.MinKey()) >= 0
		})
		// todo 看不懂
		/*
			n := len(s.tables)
			idx = n - 1 - sort.Search(n, func(i int) bool {
				return utils.CompareKeys(s.tables[n-1-i].ss.MinKey(), key) <= 0
			})
		*/
	}
	if idx >= len(conIter.tables) || idx < 0 {
		conIter.setIdx(-1)
		return
	}
	conIter.setIdx(idx)
	conIter.curIer.Seek(key)
}

func (conIter *ConcatIterator) Next() {
	conIter.curIer.Next()
	if conIter.curIer.Valid() {
		return
	}
	for {
		if conIter.opt.IsAsc {
			conIter.setIdx(conIter.idx + 1)
		} else {
			conIter.setIdx(conIter.idx - 1)
		}
		if conIter.curIer == nil {
			return
		}
		conIter.curIer.Rewind()
		if conIter.curIer.Valid() {
			break
		}
	}
}

func (conIter *ConcatIterator) Close() error {
	for _, t := range conIter.tables {
		if err := t.DecrRef(); err != nil {
			common.Err(err)
			return err
		}
	}
	for _, t := range conIter.iters {
		if t == nil {
			continue
		}
		if err := t.Close(); err != nil {
			common.Err(err)
			return err
		}
	}
	return nil
}

type MergeIterator struct {
	left  node
	right node
	small *node

	curKey  []byte
	reverse bool
}

type node struct {
	valid bool

	entry *model.Entry
	iter  interfaces.Iterator

	merge  *MergeIterator
	concat *ConcatIterator
}
