package utils

//
//import (
//	"container/list"
//	"sync"
//)
//
//type Eviction struct {
//	Key   string
//	Value interface{}
//}
//
//type Cache struct {
//	// If len > UpperBound, cache will automatically evict down to LowerBound.
//	// If either value is 0, this behavior is disabled.
//	UpperBound      int
//	LowerBound      int
//	values          map[string]*cacheEntry
//	freqs           *list.List
//	len             int // current entry size
//	lock            *sync.Mutex
//	EvictionChannel chan<- Eviction
//}
//
//type cacheEntry struct {
//	key      string
//	value    interface{}
//	freqNode *list.Element // key val(listEntry)
//}
//
//type listEntry struct {
//	freq    int
//	entries map[*cacheEntry]byte // 记录当前频率下的 entry引用, 方便之后删除;
//}
//
//func NewCache(size int) *Cache {
//	c := new(Cache)
//	c.values = make(map[string]*cacheEntry)
//	c.freqs = list.New()
//	c.lock = new(sync.Mutex)
//	c.UpperBound = size
//	c.LowerBound = size / 2
//	c.len = 0
//	return c
//}
//
//func (c *Cache) Get(key string) (interface{}, bool) {
//	c.lock.Lock()
//	defer c.lock.Unlock()
//	if e, ok := c.values[key]; ok {
//		c.increment(e)
//		return e.value, true
//	}
//	return nil, false
//}
//
//func (c *Cache) Set(key string, value interface{}) {
//	c.lock.Lock()
//	defer c.lock.Unlock()
//	if e, ok := c.values[key]; ok {
//		// value already exists for key.  overwrite
//		e.value = value
//		c.increment(e) // 增加访问次数, 并移动元素
//	} else {
//		// value doesn't exist.  insert
//		e := new(cacheEntry)
//		e.key = key
//		e.value = value
//		c.values[key] = e
//		c.increment(e)
//		c.len++
//		// bounds mgmt try to evict.
//		if c.UpperBound > 0 && c.LowerBound > 0 {
//			if c.len > c.UpperBound {
//				// 剔除一定数量的 entry 数据
//				c.evict(c.len - c.LowerBound)
//			}
//		}
//	}
//}
//
//func (c *Cache) Len() int {
//	c.lock.Lock()
//	defer c.lock.Unlock()
//	return c.len
//}
//
//func (c *Cache) Evict(count int) int {
//	c.lock.Lock()
//	defer c.lock.Unlock()
//	return c.evict(count)
//}
//
//func (c *Cache) evict(count int) int {
//	// No lock here so it can be called from within the lock (during Set)
//	var evicted int
//	for i := 0; i < count; {
//		if place := c.freqs.Front(); place != nil {
//			// 直接就可以知道 当前频率下 所涉及到的 所有 entry;
//			for entry, _ := range place.Value.(*listEntry).entries {
//				if i < count {
//					if c.EvictionChannel != nil {
//						c.EvictionChannel <- Eviction{
//							Key:   entry.key,
//							Value: entry.value,
//						}
//					}
//					delete(c.values, entry.key)
//					c.remEntry(place, entry)
//					evicted++
//					c.len--
//					i++
//				}
//			}
//		}
//	}
//	return evicted
//}
//
//// key1 entry1
//// key2 entry2
//
//// head frepNode1  frepNode2  frepNode3  frepNode4
//// val    entry1     entry2    entry3      entry4
//func (c *Cache) increment(e *cacheEntry) {
//	currentPlace := e.freqNode
//	var nextFreq int
//	var nextPlace *list.Element
//	if currentPlace == nil { // 新增的 entry
//		// new entry to put the after head
//		nextFreq = 1
//		nextPlace = c.freqs.Front() // put新元素时,将元素放在首部;
//	} else {
//		// move up
//		nextFreq = currentPlace.Value.(*listEntry).freq + 1
//		nextPlace = currentPlace.Next()
//	}
//
//	// 1. 首次添加
//	// 2. +1的节点不存在, 需要创建
//	if nextPlace == nil || nextPlace.Value.(*listEntry).freq != nextFreq {
//		// create a new freqList entry
//		li := new(listEntry)
//		li.freq = nextFreq
//		li.entries = make(map[*cacheEntry]byte)
//
//		if currentPlace != nil {
//			nextPlace = c.freqs.InsertAfter(li, currentPlace)
//		} else {
//			nextPlace = c.freqs.PushFront(li)
//		}
//	}
//
//	// 3. 移动元素
//	e.freqNode = nextPlace
//	nextPlace.Value.(*listEntry).entries[e] = 1
//
//	if currentPlace != nil {
//		// remove from current position
//		c.remEntry(currentPlace, e)
//	}
//}
//
//func (c *Cache) remEntry(place *list.Element, entry *cacheEntry) {
//	entries := place.Value.(*listEntry).entries
//	delete(entries, entry)
//	if len(entries) == 0 {
//		c.freqs.Remove(place)
//	}
//}
//
//func (c *Cache) Close() error {
//	if c.EvictionChannel != nil {
//		close(c.EvictionChannel)
//	}
//	c.freqs = nil
//	c.values = nil
//	return nil
//}
