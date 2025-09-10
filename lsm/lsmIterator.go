package lsm

import (
	"bytes"
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/model"
	"sort"
)

type lsmIterator struct {
	iters []model.Iterator
	item  model.Item
}

func (lsm *LSM) NewLsmIterator(opt *model.Options) []model.Iterator {
	iter := &lsmIterator{}
	iter.iters = make([]model.Iterator, 0)
	iter.iters = append(iter.iters, lsm.memoryTable.skipList.NewSkipListIterator(lsm.memoryTable.name))
	for _, imemoryTable := range lsm.immemoryTables {
		iter.iters = append(iter.iters, imemoryTable.skipList.NewSkipListIterator(imemoryTable.name))
	}
	iter.iters = append(iter.iters, lsm.LevelManger.iterators(opt)...)
	return iter.iters
}

type ConcatIterator struct {
	tables []*Table
	iters  []model.Iterator
	idx    int
	curIer model.Iterator
	opt    *model.Options
}

func NewConcatIterator(tables []*Table, opt *model.Options) *ConcatIterator {
	for i := 0; i < len(tables); i++ {
		tables[i].IncrRef()
	}
	return &ConcatIterator{
		tables: tables,
		iters:  make([]model.Iterator, len(tables)),
		idx:    -1,
		curIer: nil,
		opt:    opt,
	}
}
func (s *ConcatIterator) Name() string {
	return s.curIer.Name()
}
func (conIter *ConcatIterator) Value() []byte {
	return conIter.Item().Item.Value
}
func (conIter *ConcatIterator) Key() []byte {
	return conIter.Item().Item.Key
}
func (conIter *ConcatIterator) setIdx(inx int) {
	conIter.idx = inx
	if inx < 0 || inx >= len(conIter.tables) {
		conIter.curIer = nil
		return
	}
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
		// 升序, 设置第0个table开始;
		conIter.setIdx(0)
	} else {
		conIter.setIdx(len(conIter.iters) - 1)
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
	if conIter.opt.IsAsc { // 升序遍历;
		idx = sort.Search(len(conIter.tables), func(i int) bool {
			maxKey := conIter.tables[i].sst.MaxKey()
			cmp := model.CompareKeyNoTs(maxKey, key) >= 0
			return cmp
		})
	} else { // 降序遍历; sst 本身是 升序的; 需要从后往前找,小于当前值的;
		//idx = sort.Search(len(conIter.tables), func(i int) bool {
		//	minKey := conIter.tables[i].sst.MinKey()
		//	cmp := model.CompareKeyNoTs(key, minKey) >= 0
		//	return cmp
		//})
		n := len(conIter.tables)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return model.CompareKeyNoTs(conIter.tables[n-1-i].sst.MinKey(), key) <= 0
		})
	}
	if idx >= len(conIter.tables) || idx < 0 {
		conIter.setIdx(-1)
		return
	}
	conIter.setIdx(idx)
	conIter.curIer.Seek(key)
}
func (conIter *ConcatIterator) Next() {
	// 当前 table 向后找一个 block;
	conIter.curIer.Next() // 当前 table迭代器向后找一个;
	// 假如当前 block 有效, 则直接返回;
	if conIter.curIer.Valid() {
		return
	}
	// 如果向后找一个block无效了; 开始找下一个 table;
	for {
		if conIter.opt.IsAsc { // 升序,直接找后一个;
			conIter.setIdx(conIter.idx + 1)
		} else { // 降序找前一个;
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
	left    node
	right   node
	small   *node
	lsm     *LSM
	curKey  []byte
	curVal  []byte
	reverse bool
}

type node struct {
	valid bool

	entry model.Entry
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
		n.valid = n.merge.small.valid
		if n.valid {
			n.entry = n.merge.small.entry
		}
	case n.concat != nil:
		n.valid = n.concat.Valid()
		if n.valid {
			n.entry = n.concat.Item().Item
		}
	default:
		n.valid = n.iter.Valid()
		if n.valid {
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
func (m *MergeIterator) Name() string {
	return m.small.iter.Name()
}
func (m *MergeIterator) fix() {
	if !m.otherNode().valid {
		return
	}
	if !m.small.valid {
		m.swapSmall()
		return
	}
	// 这里应该全量对比, 具体去留让 compact组件 定夺;
	cmp := model.CompareKeyWithTs(m.small.entry.Key, m.otherNode().entry.Key)
	switch {
	case cmp == 0:
		m.otherNode().next()
		return
	case cmp < 0:
		if m.reverse {
			m.swapSmall()
		} else {
		}
	default:
		if m.reverse {
		} else {
			m.swapSmall()
		}
	}
}
func (m *MergeIterator) swapSmall() {
	if m.small == &m.left {
		m.small = &m.right
	} else {
		m.small = &m.left
	}
}
func (m *MergeIterator) otherNode() *node {
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
		m.small.next() // n.setEntry();
		m.fix()        // small 和 otherNode 相比较;
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
func (m *MergeIterator) Key() []byte {
	return m.Item().Item.Key
}
func (m *MergeIterator) Value() []byte {
	return m.Item().Item.Value
}
func (m *MergeIterator) Rewind() {
	m.left.Rewind()   // n.setEntry();
	m.right.Rewind()  // n.setEntry();
	m.fix()           // 挑选最小值, 另外一个可能 next();
	m.setCurrentKey() // 赋值最小值;
}
func (m *MergeIterator) setCurrentKey() {
	common.CondPanic(m.small.entry.Key == nil && m.small.valid == true,
		fmt.Errorf("mi.small.entry is nil"))
	if m.small.valid {
		m.curKey = append(m.curKey[:0], m.small.entry.Key...)
		m.curVal = append(m.curVal[:0], m.small.entry.Value...)
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
