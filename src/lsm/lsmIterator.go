package lsm

import (
	"bytes"
	"fmt"
	"sort"
	"trainKv/common"
	"trainKv/model"
)

type lsmIterator struct {
	iters []model.Iterator
	item  model.Item
}

func (lsm *LSM) NewLsmIterator() []model.Iterator {
	iter := &lsmIterator{}
	iter.iters = make([]model.Iterator, 0)
	iter.iters = append(iter.iters, lsm.memoryTable.skipList.NewSkipListIterator())
	for _, imemoryTable := range lsm.immemoryTables {
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
func (lsmIter *lsmIterator) Item() model.Item {
	return lsmIter.iters[0].Item()
}
func (lsmIter *lsmIterator) Close() error {
	return nil
}

type ConcatIterator struct {
	tables []*table
	iters  []model.Iterator
	idx    int
	curIer model.Iterator
	opt    *model.Options
}

func NewConcatIterator(tables []*table, opt *model.Options) *ConcatIterator {
	return &ConcatIterator{
		tables: tables,
		iters:  make([]model.Iterator, len(tables)),
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

func (conIter *ConcatIterator) Item() model.Item {
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
	iter  model.Iterator

	merge  *MergeIterator
	concat *ConcatIterator
}

func (n *node) setIterator(iter model.Iterator) {
	n.iter = iter
	n.merge, _ = iter.(*MergeIterator)
	n.concat, _ = iter.(*ConcatIterator)
}

func (n *node) setEntry() {
	switch {
	case n.merge != nil:
		if n.merge.small.valid {
			n.entry = n.merge.small.entry
		}
	case n.concat != nil:
		if n.concat.Valid() {
			n.entry = n.concat.Item().Item
		}
	default:
		if n.iter.Valid() {
			n.entry = n.iter.Item().Item
		}
	}
}

func (n *node) next() {
	switch {
	case n.merge != nil:
		n.merge.Next()
	case n.concat != nil:
		n.concat.Next()
	default:
		n.iter.Next()
	}
	n.setEntry()
}

func (n *node) Rewind() {
	n.iter.Rewind()
	n.setEntry()
}

func (n *node) seek(key []byte) {
	n.iter.Seek(key)
	n.setEntry()
}

func NewMergeIterator(iters []model.Iterator, reverse bool) model.Iterator {
	switch len(iters) {
	case 0:
		return nil
	case 1:
		return iters[0]
	case 2:
		m := &MergeIterator{
			reverse: reverse,
		}
		m.left.setIterator(iters[0])
		m.right.setIterator(iters[1])
		m.small = &m.left
		return m
	}
	mid := len(iters) / 2
	return NewMergeIterator([]model.Iterator{
		NewMergeIterator(iters[:mid], reverse),
		NewMergeIterator(iters[mid:], reverse)}, reverse)
}

func (m *MergeIterator) fix() {
	if !m.otherNode().valid {
		return
	}
	if !m.small.valid {
		m.swapSmall()
		return
	}
	cmp := model.CompareKey(m.small.entry.Key, m.otherNode().entry.Key)
	switch {
	case cmp == 0:
		m.otherNode().next()
	case cmp > 0:
		if m.reverse {
		} else {
			m.swapSmall()
		}
	case cmp < 0:
		if m.reverse {
			m.swapSmall()
		}
	}
}

func (m MergeIterator) swapSmall() {
	if m.small == &m.left {
		m.small = &m.right
	} else {
		m.small = &m.left
	}
}

func (m MergeIterator) otherNode() *node {
	if &m.left == m.small {
		return &m.right
	} else {
		return &m.left
	}
}

func (m *MergeIterator) Next() {
	for m.small.valid {
		if !bytes.Equal(m.small.entry.Key, m.curKey) {
			break
		}
		m.small.next()
		m.fix()
	}
	m.setCurrentKey()
}

func (m *MergeIterator) Seek(key []byte) {
	m.left.seek(key)
	m.right.seek(key)
	m.fix()
	m.setCurrentKey()
}

func (m *MergeIterator) Item() model.Item {
	return m.small.iter.Item()
}
func (m *MergeIterator) Rewind() {
	m.left.Rewind()
	m.right.Rewind()
	m.fix()
	m.setCurrentKey()
}
func (m MergeIterator) setCurrentKey() {
	common.CondPanic(m.small.entry == nil && m.small.valid == true,
		fmt.Errorf("mi.small.entry is nil"))
	if m.small.valid {
		m.curKey = append(m.curKey[:0], m.small.entry.Key...)
	}
}
func (m *MergeIterator) Valid() bool {
	return m.small.valid
}

func (m *MergeIterator) Close() error {
	if err := m.left.iter.Close(); err != nil {
		return common.WarpErr("MergeIterator.Close", err)
	}
	if err := m.right.iter.Close(); err != nil {
		return common.WarpErr("MergeIterator.Close", err)
	}
	return nil
}
