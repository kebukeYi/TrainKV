package cache

import (
	"container/list"
	"fmt"
)

type winLRU struct {
	data map[uint64]*list.Element
	cap  int
	list *list.List
}

type storeItem struct {
	keyHash uint64
	value   interface{}
	stage   int
}

func NewWinLRU(size int, data map[uint64]*list.Element) *winLRU {
	return &winLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

func (wlru *winLRU) add(newItem storeItem) (eitem storeItem, evicted bool) {
	if wlru.list.Len() < wlru.cap {
		wlru.data[newItem.keyHash] = wlru.list.PushFront(&newItem)
		return storeItem{}, false
	}

	evictItem := wlru.list.Back()
	item := evictItem.Value.(*storeItem)

	delete(wlru.data, item.keyHash)

	eitem, *item = *item, newItem

	wlru.data[item.keyHash] = evictItem
	wlru.list.MoveToFront(evictItem)
	return eitem, true
}

func (wlru *winLRU) get(v *list.Element) {
	wlru.list.MoveToFront(v)
}

func (wlru winLRU) string() string {
	s := "winlru:["
	for item := wlru.list.Front(); item != nil; item = item.Next() {
		s += fmt.Sprintf("%v, ", item.Value.(*storeItem).value)
	}
	s += "]"
	return s
}
