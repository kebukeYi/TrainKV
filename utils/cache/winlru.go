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

func (wlru *winLRU) add(newItem storeItem) (oldItem storeItem, evicted bool) {
	// 如果还有空间, 那么添加;
	if wlru.list.Len() < wlru.cap {
		wlru.data[newItem.keyHash] = wlru.list.PushFront(&newItem)
		return storeItem{}, false
	}

	// 没有空间, 那么就删除最旧的数据;
	evictItem := wlru.list.Back()
	item := evictItem.Value.(*storeItem)

	delete(wlru.data, item.keyHash)

	oldItem, *item = *item, newItem

	wlru.data[item.keyHash] = evictItem
	wlru.list.MoveToFront(evictItem)
	return oldItem, true
}

func (wlru *winLRU) get(v *list.Element) {
	wlru.list.MoveToFront(v)
}

func (wlru *winLRU) len() int {
	return wlru.list.Len()
}

func (wlru *winLRU) string() string {
	s := "winlru:["
	for item := wlru.list.Front(); item != nil; item = item.Next() {
		s += fmt.Sprintf("%v, ", item.Value.(*storeItem).value)
	}
	s += "]"
	return s
}
