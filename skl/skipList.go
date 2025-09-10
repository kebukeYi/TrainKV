package skl

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/pkg/errors"
	"log"
	"math"
	"strings"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

const MaxSkipNodeSize = int(unsafe.Sizeof(skipNode{}))

type skipNode struct {
	// 前32位是在arena空间里的首地址,后32位是size;
	// 地址空间内容包括: meta, val, expireAt, not version;
	value uint64

	keyOffset uint32
	keySize   uint32

	height uint16
	next   [maxHeight]uint32
}

func newSkipNode(arena *Arena, key []byte, value model.ValueExt, height int) *skipNode {
	nodeOffset := arena.AllocateNode(height)
	keyOffset := arena.PutKey(key)
	valOffset := arena.PutVal(value)

	node := arena.getNode(nodeOffset)
	node.height = uint16(height)
	node.keyOffset = keyOffset
	node.keySize = uint32(len(key))
	node.value = encodeVal(valOffset, value.EncodeValSize())
	return node
}
func encodeVal(valOffset, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}
func decodeVal(val uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(val)
	valSize = uint32(val >> 32)
	return
}
func (n *skipNode) getValOffset() (uint32, uint32) {
	loadUint64 := atomic.LoadUint64(&n.value)
	return decodeVal(loadUint64)
}
func (n *skipNode) getKey(arena *Arena) []byte {
	key := arena.getKey(n.keyOffset, n.keySize)
	return key
}
func (n *skipNode) setVal(val uint64) {
	atomic.StoreUint64(&n.value, val)
}
func (n *skipNode) getNextOffset(height int) uint32 {
	return atomic.LoadUint32(&n.next[height])
}
func (n *skipNode) casNextOffset(h int, old, new uint32) bool {
	return atomic.CompareAndSwapUint32(&n.next[h], old, new)
}
func (n *skipNode) getVal(arena *Arena) model.ValueExt {
	return arena.getVal(n.getValOffset())
}

type SkipList struct {
	arena      *Arena
	num        int32
	headOffset uint32
	height     int32
	ref        atomic.Int32
	OnClose    func()
}

func NewSkipList(arenaSize int64) *SkipList {
	arena := NewArena(arenaSize)
	head := newSkipNode(arena, nil, model.ValueExt{}, maxHeight)
	headOffset := arena.getNodeOffset(head)
	skip := &SkipList{
		arena:      arena,
		num:        0,
		headOffset: headOffset,
		height:     1,
	}
	skip.ref.Store(1)
	return skip
}
func (skipList *SkipList) IncrRef() {
	skipList.ref.Add(1)
}
func (skipList *SkipList) DecrRef() {
	newRef := skipList.ref.Add(-1)
	if newRef > 0 {
		return
	}
	if skipList.OnClose != nil {
		skipList.OnClose()
	}
	skipList.arena = nil
	skipList.headOffset = 0
}
func (skipList *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && model.FastRand() <= heightIncrease {
		h++
	}
	return h
}
func (skipList *SkipList) getNextNode(n *skipNode, h int) *skipNode {
	if n == nil {
		return nil
	}
	return skipList.arena.getNode(n.getNextOffset(h))
}
func (skipList *SkipList) getHead() *skipNode {
	return skipList.arena.getNode(skipList.headOffset)
}
func (skipList *SkipList) getHeight() int32 {
	return atomic.LoadInt32(&skipList.height)
}
func (skipList *SkipList) GetMemSize() int64 {
	//return skipList.arena.size()
	return int64(skipList.num)
}
func (skipList *SkipList) findLast() *skipNode {
	n := skipList.getHead()
	level := int(skipList.getHeight() - 1)
	for {
		next := skipList.getNextNode(n, level)
		if next != nil {
			n = next
			continue
		}
		// next == nil
		if level == 0 {
			if n == skipList.getHead() {
				return nil
			}
			return n
		}
		level--
	}
}
func (skipList *SkipList) Empty() bool {
	return skipList.findLast() == nil
}
func (skipList *SkipList) findNear(key []byte, less bool, allowEqual bool) (*skipNode, bool) {
	x := skipList.getHead()
	level := int(skipList.getHeight() - 1)
	for {
		nextNode := skipList.getNextNode(x, level)
		if nextNode == nil {
			if level > 0 {
				level--
				continue
			}
			// level = 0
			if !less {
				return nil, false
			}
			// level = 0 ; less = true
			if x == skipList.getHead() {
				return nil, false
			}
			return x, false
		}
		// nextNode!=nil
		getKey := nextNode.getKey(skipList.arena)
		cmp := model.CompareKeyNoTs(key, getKey)
		if cmp > 0 {
			x = nextNode
			continue
		}
		if cmp == 0 {
			if allowEqual {
				return nextNode, true
			}
			// allowEqual = false; less = true =>
			if !less {
				return skipList.getNextNode(nextNode, 0), false
			}
			// allowEqual = false; less = false =>
			if level > 0 {
				level--
				continue
			}
			// allowEqual = false; less = false; level = 0 =>
			if x == skipList.getHead() {
				return nil, false
			}
			return x, false
		}
		// cmp < 0
		if level > 0 {
			level--
			continue
		}
		// cmp < 0 ; level = 0 => At base level. Need to return something.
		if !less {
			return nextNode, false
		}
		// Try to return x. Make sure it is not a head node.
		if x == skipList.getHead() {
			return nil, false
		}
		return x, false
	}
}
func (skipList *SkipList) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	for {
		beforeNode := skipList.arena.getNode(before)
		nextOffset := beforeNode.getNextOffset(level)
		nextNode := skipList.arena.getNode(nextOffset)
		if nextNode == nil {
			return before, nextOffset
		}
		nextKey := nextNode.getKey(skipList.arena)
		// todo 跳表对比key时, 祛除掉 ts版本号;
		cmp := model.CompareKeyNoTs(key, nextKey)
		if cmp == 0 {
			return nextOffset, nextOffset
		}
		if cmp < 0 {
			return before, nextOffset
		}
		// cmp > 0
		before = nextOffset
	}
}
func (skipList *SkipList) Get(userKeyTs []byte) model.ValueExt {
	findNear, _ := skipList.findNear(userKeyTs, false, true)
	if findNear == nil {
		return model.ValueExt{Version: -1}
	}
	nearKeyTs := skipList.arena.getKey(findNear.keyOffset, findNear.keySize)
	if !model.SameKeyNoTs(userKeyTs, nearKeyTs) {
		return model.ValueExt{Version: -1}
	}
	valOffset, valSize := findNear.getValOffset()
	val := skipList.arena.getVal(valOffset, valSize)
	// todo 非常重要的一步, 需要显式分析出版本,以便在vlogGC中进行版本判断,决定去留;
	// 此时的 nearKeyTs 的版本时间戳 可能是大于等于 userKeyTs的; 因此需要显示解析出确定的版本号;
	val.Version = model.ParseTsVersion(nearKeyTs)
	return val
}
func (skipList *SkipList) Put(e *model.Entry) {
	key, v := e.Key, model.ValueExt{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
		Version:   e.Version,
	}

	listHeight := skipList.getHeight()
	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32
	prev[listHeight] = skipList.headOffset
	for i := int(listHeight) - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		prev[i], next[i] = skipList.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			vo := skipList.arena.PutVal(v)
			encValue := encodeVal(vo, v.EncodeValSize())
			prevNode := skipList.arena.getNode(prev[i])
			prevNode.setVal(encValue)
			atomic.AddInt32(&skipList.num, 1)
			return
		}
	}

	height := skipList.randomHeight()
	x := newSkipNode(skipList.arena, key, v, height)

	listHeight = skipList.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&skipList.height, listHeight, int32(height)) {
			break
		}
		listHeight = skipList.getHeight()
	}

	for i := 0; i < height; i++ {
		for {
			if skipList.arena.getNode(prev[i]) == nil {
				AssertTrue(i > 1) // This cannot happen in base level.
				prev[i], next[i] = skipList.findSpliceForLevel(key, skipList.headOffset, i)
				AssertTrue(prev[i] != next[i])
			}
			x.next[i] = next[i]
			pnode := skipList.arena.getNode(prev[i])
			if pnode.casNextOffset(i, next[i], skipList.arena.getNodeOffset(x)) {
				if i == 0 {
					atomic.AddInt32(&skipList.num, 1)
				}
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			prev[i], next[i] = skipList.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				vo := skipList.arena.PutVal(v)
				encValue := encodeVal(vo, v.EncodeValSize())
				prevNode := skipList.arena.getNode(prev[i])
				prevNode.setVal(encValue)
				atomic.AddInt32(&skipList.num, 1)
				return
			}
		}
	}
}
func (skipList *SkipList) Draw(align bool) {
	reverseTree := make([][]string, skipList.getHeight())
	head := skipList.getHead()
	for level := int(skipList.getHeight()) - 1; level >= 0; level-- {
		next := head
		for {
			var nodeStr string
			next = skipList.getNextNode(next, level)
			if next != nil {
				key := next.getKey(skipList.arena)
				vs := next.getVal(skipList.arena)
				nodeStr = fmt.Sprintf("%s(%s)", model.ParseKey(key), vs.Value)
			} else {
				break
			}
			reverseTree[level] = append(reverseTree[level], nodeStr)
		}
	}

	if align && skipList.getHeight() > 1 {
		baseFloor := reverseTree[0]
		for level := 1; level < int(skipList.getHeight()); level++ {
			pos := 0
			for _, ele := range baseFloor {
				if pos == len(reverseTree[level]) {
					break
				}
				if ele != reverseTree[level][pos] {
					newStr := fmt.Sprintf(strings.Repeat("-", len(ele)))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newStr
				}
				pos++
			}
		}
	}

	for level := int(skipList.getHeight()) - 1; level >= 0; level-- {
		fmt.Printf("%d: ", level)
		for pos, ele := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%s  ", ele)
			} else {
				fmt.Printf("%s->", ele)
			}
		}
		fmt.Println()
	}
}
func (skipList *SkipList) NewSkipListIterator(name string) model.Iterator {
	skipList.IncrRef()
	return &SkipListIterator{
		list: skipList,
		name: name,
	}
}

type SkipListIterator struct {
	name string
	list *SkipList
	curr *skipNode
}

func (s *SkipListIterator) Name() string {
	return s.name
}
func (s *SkipListIterator) Key() []byte {
	return s.list.arena.getKey(s.curr.keyOffset, s.curr.keySize)
}
func (s *SkipListIterator) Value() model.ValueExt {
	valOffset, valSize := s.curr.getValOffset()
	val := s.list.arena.getVal(valOffset, valSize)
	return val
}
func (s *SkipListIterator) ValueUint64() uint64 {
	return s.curr.value
}
func (s *SkipListIterator) Next() {
	//AssertTrue(s.Valid())
	if s.Valid() {
		s.curr = s.list.getNextNode(s.curr, 0)
	} else {
		s.curr = nil
		return
	}
}
func (s *SkipListIterator) Prev() {
	AssertTrue(s.Valid())
	s.curr, _ = s.list.findNear(s.Key(), true, false)
}
func (s *SkipListIterator) Valid() bool {
	return s.curr != nil
}
func (s *SkipListIterator) Rewind() {
	s.SeekToFirst()
}
func (s *SkipListIterator) Item() model.Item {
	return model.Item{Item: model.Entry{
		Key:       s.Key(),
		Value:     s.Value().Value,
		Meta:      s.Value().Meta,
		ExpiresAt: s.Value().ExpiresAt,
		Version:   s.Value().Version,
	}}
}
func (s *SkipListIterator) Seek(key []byte) {
	// find >=
	s.curr, _ = s.list.findNear(key, false, true)
}
func (s *SkipListIterator) SeekForPrev(target []byte) {
	// find <=
	s.curr, _ = s.list.findNear(target, true, true)
}
func (s *SkipListIterator) SeekToFirst() {
	s.curr = s.list.getNextNode(s.list.getHead(), 0) // 有可能是 nil
}
func (s *SkipListIterator) SeekToLast() {
	s.curr = s.list.findLast()
}
func (s *SkipListIterator) Close() error {
	s.list.DecrRef()
	return nil
}

func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}
