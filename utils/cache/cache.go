package cache

import (
	"container/list"
	"strings"
	"sync"

	"github.com/kebukeYi/TrainKV/v2/utils"
)

// cacheShardBits 分片数的对数: 2^4 = 16 个分片;
// 块缓存的每次访问 (命中/未命中) 都要拿写锁 (LRU 晋升/准入统计会改状态),
// 全局单锁在 compaction 协程与读协程并发时会退化为 futex 竞争 (profile 中约占读路径 13%),
// 分片后每把锁的流量降为 1/16, 竞争基本消除;
const cacheShardBits = 4

// Cache 分片路由: 按 key 哈希路由到 16 个独立的 W-TinyLFU 分片;
type Cache struct {
	shards []*cacheShard
}

// cacheShard 单个分片: 完整的 W-TinyLFU (窗口 LRU + 分段 LRU + 布隆准入 + CM-sketch);
type cacheShard struct {
	m         sync.RWMutex
	wlru      *winLRU
	slru      *segmentedLRU
	door      *BloomFilter
	cmkt      *cmSketch
	total     int32
	threshold int32
	data      map[uint64]*list.Element
}

func newCacheShard(numEntries int) *cacheShard {
	const winlruPct = 15 // 占比15%;
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
	return &cacheShard{
		wlru:      NewWinLRU(winlruSz, data),
		slru:      newSLRU(data, slruOne, slruTwo),
		door:      newBloomFilter(numEntries, 0.005),
		cmkt:      newCmSketch(int64(numEntries) * 2),
		total:     0,
		threshold: int32(numEntries * 100),
		data:      data,
	}
}

func NewCache(numEntries int) *Cache {
	numShards := 1 << cacheShardBits
	perShard := numEntries / numShards
	if perShard < 1 {
		perShard = 1
	}
	c := &Cache{shards: make([]*cacheShard, numShards)}
	for i := range c.shards {
		c.shards[i] = newCacheShard(perShard)
	}
	return c
}

// shardFor 乘法散列路由: 块缓存 key 是 fid<<32|blockIdx 的结构化数值,
// 直接取低位会集中在少数分片, 乘黄金比例常数后取高位可均匀分散;
func (c *Cache) shardFor(hash uint64) *cacheShard {
	return c.shards[(hash*0x9E3779B97F4A7C15)>>(64-cacheShardBits)]
}

func (c *Cache) Set(key, val interface{}) bool {
	keyToHash, _ := utils.KeyToHash(key)
	shard := c.shardFor(keyToHash)
	shard.m.Lock()
	defer shard.m.Unlock()
	return shard.set(keyToHash, val)
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	keyToHash, _ := utils.KeyToHash(key)
	shard := c.shardFor(keyToHash)
	shard.m.Lock()
	defer shard.m.Unlock()
	return shard.get(keyToHash)
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	keyToHash, _ := utils.KeyToHash(key)
	shard := c.shardFor(keyToHash)
	shard.m.Lock()
	defer shard.m.Unlock()
	return shard.del(keyToHash)
}

func (c *Cache) Len() int {
	total := 0
	for _, shard := range c.shards {
		total += shard.len()
	}
	return total
}

func (c *Cache) String() string {
	var sb strings.Builder
	for i, shard := range c.shards {
		sb.WriteString(shard.string())
		if i < len(c.shards)-1 {
			sb.WriteString(" | ")
		}
	}
	return sb.String()
}

func (c *Cache) Stats() map[string]interface{} {
	win, slru, total := 0, 0, 0
	var accesses int32
	for _, shard := range c.shards {
		shard.m.RLock()
		win += shard.wlru.len()
		slru += shard.slru.len()
		accesses += shard.total
		shard.m.RUnlock()
	}
	total = win + slru
	return map[string]interface{}{
		"win_lru_size":   win,
		"slru_size":      slru,
		"total_size":     total,
		"total_accesses": accesses,
		"hit_count":      accesses - int32(total), // 简化计算
	}
}

// ---------------- 分片内部实现 (调用方须持有 shard.m) ----------------

func (s *cacheShard) set(hash uint64, val interface{}) bool {
	// 判断是否更新操作;
	if _, ok := s.get(hash); ok {
		// get 已完成计数与晋升, 此处按最新映射更新值 (晋升可能交换过 list.Element);
		s.data[hash].Value.(*storeItem).value = val
		return true
	}
	item := storeItem{
		keyHash: hash,
		value:   val,
		stage:   Win_LRU,
	}
	eitem, evicted := s.wlru.add(item)
	if !evicted {
		return true
	}
	// 如果 winlru 中有被淘汰的数据, 会走到这里
	// 需要从 LFU 的 stageOne 部分找到一个淘汰者(未剔除)
	// 二者进行 PK
	victim := s.slru.victim()
	if victim == nil {
		s.slru.add(eitem)
		return true
	}
	// 这里进行 PK，必须在 bloomFilter 中出现过一次, 才允许 PK
	// 在 bf 中出现, 说明访问频率 >= 2
	if !s.door.Allow(uint32(eitem.keyHash)) {
		return true
	}
	vcount := s.cmkt.Estimate(victim.keyHash)
	icount := s.cmkt.Estimate(eitem.keyHash)
	if vcount > icount {
		return true
	}

	// 执行到这里 说明 winlru 的值频率>= slru 的值频率; 需要留下 winlru 的值;
	// 留下来的 进入 stageOne, 但是此时 victim 并没有剔除掉, 但是add()方法的逻辑中会进行剔除判断;
	s.slru.add(eitem)
	return true
}

func (s *cacheShard) get(hash uint64) (interface{}, bool) {
	s.total++
	if s.total == s.threshold {
		s.cmkt.Reset()
		s.door.reset()
		s.total = 0
	}

	element, ok := s.data[hash]

	// 全局缓存中不存在;
	if !ok {
		// todo 自动更换热点数据 关键点
		// 不存在也要记录对应的数据频率, 说明是需要下一步进行缓存的;
		// 这样积累的访问次数会 逐渐 替换掉上个阶段内 需要淘汰的`伪高频`数据;
		s.door.Allow(uint32(hash))
		s.cmkt.increment(hash)
		return nil, false
	}

	item := element.Value.(*storeItem)
	s.door.Allow(uint32(hash))
	s.cmkt.increment(item.keyHash)

	if item.stage == Win_LRU {
		s.wlru.get(element)
	} else {
		s.slru.get(element)
	}
	// slru.get 晋升时可能交换两个 list.Element 的 storeItem (stageOne→stageTwo 且 stageTwo 已满),
	// 使 key→element 的映射发生改变; 必须重新解析, 否则会返回交换后对方 key 的 value;
	element, ok = s.data[hash]
	if !ok {
		return nil, false
	}
	return element.Value.(*storeItem).value, true
}

func (s *cacheShard) del(hash uint64) (interface{}, bool) {
	element, ok := s.data[hash]
	if !ok {
		return nil, false
	}
	item := element.Value.(*storeItem)
	switch item.stage {
	case Win_LRU:
		s.wlru.list.Remove(element)
	case STAGE_ONE:
		s.slru.stageOne.Remove(element)
	case STAGE_TWO:
		s.slru.stageTwo.Remove(element)
	}
	delete(s.data, hash)
	return hash, true
}

func (s *cacheShard) len() int {
	return s.wlru.len() + s.slru.len()
}

func (s *cacheShard) string() string {
	return s.wlru.string() + " | " + s.slru.string()
}
