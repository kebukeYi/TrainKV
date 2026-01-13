package cache

import (
	"container/list"
	"github.com/kebukeYi/TrainKV/v2/utils"
	"sync"
)

type Cache struct {
	m         sync.RWMutex
	wlru      *winLRU
	slru      *segmentedLRU
	door      *BloomFilter
	cmkt      *cmSketch
	total     int32
	threshold int32
	data      map[uint64]*list.Element
}

func NewCache(numEntries int) *Cache {
	const winlruPct = 15
	winlruSz := (winlruPct * numEntries) / 100
	if winlruSz < 1 {
		winlruSz = 1
	}
	slruSz := int(float64(numEntries) * ((100 - winlruPct) / 100.0))
	if slruSz < 1 {
		slruSz = 1
	}
	slruOne := int(0.15 * float64(slruSz))
	if slruOne < 1 {
		slruOne = 1
	}
	slruTwo := slruSz - slruOne
	if slruTwo < 1 {
		slruTwo = 1
	}
	data := make(map[uint64]*list.Element, numEntries)
	return &Cache{
		m:         sync.RWMutex{},
		wlru:      NewWinLRU(winlruSz, data),
		slru:      newSLRU(data, slruOne, slruTwo),
		door:      newBloomFilter(numEntries, 0.005),
		cmkt:      newCmSketch(int64(numEntries) * 2),
		total:     0,
		threshold: int32(numEntries * 100),
		data:      data,
	}
}

func (c *Cache) Set(key, val interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, val)
}

func (c *Cache) set(key, val interface{}) bool {
	keyToHash, _ := utils.KeyToHash(key)
	// 判断是否更新操作;
	element, ok := c.get(keyToHash)
	if ok {
		element.Value.(*storeItem).value = val
		return true
	}
	item := storeItem{
		keyHash: keyToHash,
		value:   val,
		stage:   Win_LRU,
	}
	eitem, evicted := c.wlru.add(item)
	if !evicted {
		return true
	}
	// 如果 winlru 中有被淘汰的数据, 会走到这里
	// 需要从 LFU 的 stageOne 部分找到一个淘汰者(未剔除)
	// 二者进行 PK
	victim := c.slru.victim()
	if victim == nil {
		c.slru.add(eitem)
		return true
	}
	// 这里进行 PK，必须在 bloomFilter 中出现过一次, 才允许 PK
	// 在 bf 中出现, 说明访问频率 >= 2
	if !c.door.Allow(uint32(eitem.keyHash)) {
		return true
	}
	vcount := c.cmkt.Estimate(victim.keyHash)
	icount := c.cmkt.Estimate(eitem.keyHash)
	if vcount > icount {
		return true
	}

	// 执行到这里 说明 winlru 的值频率>= slru 的值频率; 需要留下 winlru 的值;
	// 留下来的 进入 stageOne, 但是此时 victim 并没有剔除掉, 但是add()方法的逻辑中会进行剔除判断;
	c.slru.add(eitem)
	return true
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	element, ok := c.get(key)
	if !ok {
		return nil, false
	}
	return element.Value.(*storeItem).value, ok
}

func (c *Cache) get(key interface{}) (*list.Element, bool) {
	c.total++
	if c.total == c.threshold {
		c.cmkt.Reset()
		c.door.reset()
		c.total = 0
	}

	keyToHash, _ := utils.KeyToHash(key)
	element, ok := c.data[keyToHash]

	// 全局缓存中不存在;
	if !ok {
		// todo 自动更换热点数据 关键点
		// 不存在也要记录对应的数据频率, 说明是需要下一步进行缓存的;
		// 这样积累的访问次数会 逐渐 替换掉上个阶段内 需要淘汰的`伪高频`数据;
		c.door.Allow(uint32(keyToHash))
		c.cmkt.increment(keyToHash)
		return nil, false
	}

	item := element.Value.(*storeItem)
	c.door.Allow(uint32(keyToHash))
	c.cmkt.increment(item.keyHash)

	if item.stage == Win_LRU {
		c.wlru.get(element)
	} else {
		c.slru.get(element)
	}
	return element, true
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyToHash, _ := utils.KeyToHash(key)
	_, ok := c.data[keyToHash]
	if !ok {
		return nil, false
	}
	delete(c.data, keyToHash)
	return keyToHash, true
}

func (c *Cache) Len() int {
	return c.wlru.len() + c.slru.len()
}

func (c *Cache) String() string {
	s := ""
	s += c.wlru.string() + " | " + c.slru.string()
	return s
}

func (c *Cache) Stats() map[string]interface{} {
	c.m.RLock()
	defer c.m.RUnlock()

	return map[string]interface{}{
		"win_lru_size":   c.wlru.len(),
		"slru_size":      c.slru.len(),
		"total_size":     c.Len(),
		"total_accesses": c.total,
		"hit_count":      c.total - int32(c.Len()), // 简化计算
	}
}
