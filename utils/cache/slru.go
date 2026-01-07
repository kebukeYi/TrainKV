package cache

import (
	"container/list"
	"fmt"
)

type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne                 *list.List // 刚从 winlru 晋升，但还没“转正”
	stageTwo                 *list.List // 热点数据，长期保留
}

const (
	Win_LRU   = 0
	STAGE_ONE = 1
	STAGE_TWO = 2
)

func newSLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

func (slru *segmentedLRU) add(newitem storeItem) {
	newitem.stage = 1
	if slru.stageOne.Len() < slru.stageOneCap {
		slru.data[newitem.keyHash] = slru.stageOne.PushFront(&newitem)
		return
	}
	// 待定; stageOne满了,但是stageTwo未满, 那么在add逻辑中先都添加在stageOne中,后续再添加,那就淘汰stageOne中最后一个数据;
	if slru.Len() < slru.stageTwoCap+slru.stageOneCap {
		slru.data[newitem.keyHash] = slru.stageOne.PushFront(&newitem)
		return
	}
	e := slru.stageOne.Back()
	item := e.Value.(*storeItem)

	delete(slru.data, item.keyHash)

	*item = newitem
	slru.data[item.keyHash] = e
	slru.stageOne.MoveToFront(e)
}

func (slru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)
	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	// 访问的数据在 StageOne, 那么再次被访问到, 就需要提升到 StageTwo 中;

	// 假如 StageTwo 没满, 那么直接添加数据;
	if slru.stageTwo.Len() < slru.stageTwoCap {
		slru.stageOne.Remove(v)
		item.stage = STAGE_TWO
		slru.data[item.keyHash] = slru.stageTwo.PushFront(item)
		return
	}

	// StageTwo 满了, 需要淘汰数据;

	// 新数据加入 StageTwo，需要淘汰旧数据
	// StageTwo 中淘汰的数据不会消失，会进入 StageOne
	// StageOne 中，访问频率更低的数据，有可能会被淘汰
	twolruBack := slru.stageTwo.Back()
	bitem := twolruBack.Value.(*storeItem)

	*bitem, *item = *item, *bitem

	bitem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	slru.data[item.keyHash] = v
	slru.data[bitem.keyHash] = twolruBack

	slru.stageOne.MoveToFront(v)
	slru.stageTwo.MoveToFront(twolruBack)
}

func (slru *segmentedLRU) victim() *storeItem {
	if slru.Len() < slru.stageTwoCap+slru.stageOneCap {
		return nil
	}
	element := slru.stageOne.Back()
	return element.Value.(*storeItem)

}

func (slru *segmentedLRU) Len() int {
	return slru.stageOne.Len() + slru.stageTwo.Len()
}

func (slru *segmentedLRU) string() string {
	s := "stageOne:["
	for e := slru.stageOne.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v, ", e.Value.(*storeItem).value)
	}
	s += fmt.Sprintf("] | stageTwo:[")
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v, ", e.Value.(*storeItem).value)
	}
	s += "]"
	return s
}
