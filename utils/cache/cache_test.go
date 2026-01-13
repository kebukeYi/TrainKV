package cache

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/v2/utils"
	"math/rand"
	"testing"
	"time"
)

func TestNewCache(t *testing.T) {
	cache := NewCache(100)
	if cache == nil {
		t.Error("Expected cache to be created, got nil")
	}
}

func TestCache_Update(t *testing.T) {
	cache := NewCache(100)
	utils.AssertTrue(cache.Set("key", "value"))
	utils.AssertTrue(cache.Set("key", "newValue"))
	println(cache.Len())
	get, ok := cache.Get("key")
	utils.AssertTrue(ok)
	if get != "newValue" {
		t.Errorf("Expected value %v, got %v", "newValue", get)
	}
}

func TestCache_SetAndGet(t *testing.T) {
	cache := NewCache(100)

	// Test setting and getting a value
	key := "testKey"
	value := "testValue"

	cache.Set(key, value)

	result, ok := cache.Get(key)
	if !ok {
		t.Error("Expected key to exist in cache")
	}

	if result != value {
		t.Errorf("Expected value %v, got %v", value, result)
	}

	// Test getting a non-existent key
	_, ok = cache.Get("nonExistentKey")
	if ok {
		t.Error("Expected non-existent key to return false")
	}
}

func TestCache_Del(t *testing.T) {
	cache := NewCache(100)

	key := "testKey"
	value := "testValue"

	cache.Set(key, value)

	// Test deleting an existing key
	deletedKey, ok := cache.Del(key)
	if !ok {
		t.Error("Expected key to be deleted")
	}
	keyToHash, _ := utils.KeyToHash(key)
	if deletedKey != keyToHash { // Hash of "testKey"
		t.Errorf("Expected deleted key hash, got %v", deletedKey)
	}

	// Test deleting a non-existent key
	_, ok = cache.Del("nonExistentKey")
	if ok {
		t.Error("Expected non-existent key deletion to return false")
	}
}

func TestCache_Eviction(t *testing.T) {
	// Create a small cache to test eviction
	cache := NewCache(5)

	// Add more items than cache capacity
	keys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7"}
	values := []string{"val1", "val2", "val3", "val4", "val5", "val6", "val7"}

	for i := 0; i < len(keys); i++ {
		cache.Set(keys[i], values[i])
	}

	// Some items may have been evicted due to cache policy
	// At least recently accessed items should still be there
	val, ok := cache.Get(keys[len(keys)-1])
	if !ok {
		t.Error("Expected most recent item to be in cache")
	} else {
		if val != values[len(keys)-1] {
			t.Errorf("Expected value %v, got %v", values[len(keys)-1], val)
		}
	}
}

func TestCache_ConcurrentAccess(t *testing.T) {
	cache := NewCache(100)

	// Test concurrent access in a single-threaded test
	// In real-world usage, multiple goroutines would access the cache

	// Set some values
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")

	// get values concurrently (simulated)
	result1, ok1 := cache.Get("key1")
	result2, ok2 := cache.Get("key2")

	if !ok1 || result1 != "value1" {
		t.Errorf("Expected key1 to have value1, got %v, %v", result1, ok1)
	}

	if !ok2 || result2 != "value2" {
		t.Errorf("Expected key2 to have value2, got %v, %v", result2, ok2)
	}
}

// TestCacheHitRate 测试缓存命中率
// 总请求数: 10000
// 命中数: 8111
// 缓存命中率: 81.11%
// 缓存大小: 1000, 最终缓存长度: 1000
func TestCacheHitRate(t *testing.T) {
	var cacheSize int
	cacheSize = 1000
	cache := NewCache(cacheSize)

	totalRequests := 10000
	hitCount := 0
	requestCount := 0

	// 生成测试数据 - 模拟现实世界的访问模式（热数据和冷数据）
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < totalRequests; i++ {
		// 80% 的请求访问 20% 的热门数据
		var key int
		if rand.Float32() < 0.8 {
			hotSize := cacheSize / 5
			if hotSize < 0 {
				hotSize = 1
			}
			key = rand.Intn(hotSize) // 热数据
		} else {
			key = rand.Intn(cacheSize * 5) // 冷数据
		}

		_, found := cache.Get(key)
		requestCount++

		if found {
			hitCount++
		} else {
			// 如果没命中，放入缓存
			cache.Set(key, fmt.Sprintf("value_%d", key))
		}
	}

	hitRate := float64(hitCount) / float64(requestCount) * 100
	fmt.Printf("总请求数: %d\n", requestCount)
	fmt.Printf("命中数: %d\n", hitCount)
	fmt.Printf("缓存命中率: %.2f%%\n", hitRate)

	fmt.Printf("缓存大小: %d, 最终缓存长度: %d\n", cacheSize, cache.Len())

}

// TestCacheHitRateWithDifferentSizes 测试不同缓存大小下的命中率
// 不同缓存大小下的命中率对比:
//
//	缓存大小		命中率
//	100 		89.85%
//	500 		88.60%
//	1000 		86.16%
//	2000 		81.67%
//	5000 		73.50%
//
// 10000 		63.33%
func TestCacheHitRateWithDifferentSizes(t *testing.T) {
	sizes := []int{100, 500, 1000, 2000, 5000, 10000}
	totalRequests := 10000

	fmt.Println("不同缓存大小下的命中率对比:")
	fmt.Println("缓存大小\t\t命中率")

	for _, size := range sizes {
		cache := NewCache(size)
		hitCount := 0

		// 使用相同的随机种子保证测试一致性
		rand.Seed(42)

		for i := 0; i < totalRequests; i++ {
			// 80/20 访问模式
			var key int
			if rand.Float32() < 0.8 {
				hotSize := size / 5
				if hotSize < 0 {
					hotSize = 1
				}
				key = rand.Intn(hotSize) // 热数据
			} else {
				key = rand.Intn(size * 2) // 更大的范围模拟冷数据
			}

			_, found := cache.Get(key)
			if found {
				hitCount++
			} else {
				cache.Set(key, fmt.Sprintf("value_%d", key))
			}
		}

		hitRate := float64(hitCount) / float64(totalRequests) * 100
		fmt.Printf("%d \t\t%.2f%%\n", size, hitRate)
	}
}

// TestCacheHitRateWithFixedHotData 固定热数据范围
// 固定热数据范围下的命中率对比:
// 缓存大小		命中率
// 100 			6.90%
// 500 			33.48%
// 1000 		59.94%
// 2000 		76.51%
// 5000 		76.79%
// 10000 		76.52%
func TestCacheHitRateWithFixedHotData(t *testing.T) {
	sizes := []int{100, 500, 1000, 2000, 5000, 10000}
	const (
		totalRequests = 10000
		hotDataRange  = 1000 // 固定热数据范围
		coldDataRange = 5000 // 固定冷数据范围
	)

	fmt.Println("固定热数据范围下的命中率对比:")
	fmt.Println("缓存大小\t\t命中率")

	for _, size := range sizes {
		cache := NewCache(size)
		hitCount := 0

		for i := 0; i < totalRequests; i++ {
			var key int
			if rand.Float32() < 0.8 {
				key = rand.Intn(hotDataRange) // 固定热数据范围
			} else {
				key = rand.Intn(coldDataRange) // 固定冷数据范围
			}

			_, found := cache.Get(key)
			if found {
				hitCount++
			} else {
				cache.Set(key, fmt.Sprintf("value_%d", key))
			}
		}

		hitRate := float64(hitCount) / float64(totalRequests) * 100
		fmt.Printf("%d \t\t%.2f%%\n", size, hitRate)
	}
}

// BenchmarkCachePerformance 性能基准测试
// BenchmarkCachePerformance-4   	 1645863	       681.5 ns/op
func BenchmarkCachePerformance(b *testing.B) {
	cache := NewCache(1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % 2000 // 超出缓存大小，模拟真实场景
			cache.Set(key, fmt.Sprintf("value_%d_%d", key, b.N))
			cache.Get(key)
			i++
		}
	})
}

// TestCacheHitRateSequential 测试顺序访问模式下的命中率
// 顺序访问命中率: 100.00%
func TestCacheHitRateSequential(t *testing.T) {
	cache := NewCache(100)

	// 先填充缓存
	for i := 0; i < 100; i++ {
		cache.Set(i, fmt.Sprintf("value_%d", i))
	}

	// 顺序访问，应该有很高的命中率
	hitCount := 0
	for i := 0; i < 1000; i++ {
		_, found := cache.Get(i % 100) // 重复访问前100个键
		if found {
			hitCount++
		}
	}

	hitRate := float64(hitCount) / 1000.0 * 100
	fmt.Printf("顺序访问命中率: %.2f%%\n", hitRate)
	if hitRate < 90 {
		t.Errorf("期望高命中率，但实际只有 %.2f%%", hitRate)
	}
}

// TestCacheHitRateRandom 测试随机访问模式下的命中率
// 1000容量的缓存面对5000个随机键的访问
// 随机访问命中率: 19.01%
func TestCacheHitRateRandom(t *testing.T) {
	cache := NewCache(1000)

	// 随机访问大量不同的键
	hitCount := 0
	totalRequests := 10000

	for i := 0; i < totalRequests; i++ {
		key := rand.Intn(5000) // 5倍于缓存大小的不同键
		_, found := cache.Get(key)
		if found {
			hitCount++
		} else {
			cache.Set(key, fmt.Sprintf("value_%d", key))
		}
	}

	hitRate := float64(hitCount) / float64(totalRequests) * 100
	fmt.Printf("随机访问命中率: %.2f%%\n", hitRate)
}

// TestCacheHitRateZipfian 模拟 Zipfian 分布访问模式
// Zipfian 分布访问命中率: 90.03%
func TestCacheHitRateZipfian(t *testing.T) {
	cache := NewCache(1000)

	// 模拟 Zipfian 分布 - 大部分访问集中在少数键上
	// 80%的请求集中在20%的数据
	hitCount := 0
	totalRequests := 10000

	for i := 0; i < totalRequests; i++ {
		// 使用简单的幂律分布模拟
		// 大部分请求会落在较小的键值范围内
		r := rand.Float64()
		key := int(r * r * 1000) // 平方使小值更常见

		_, found := cache.Get(key)
		if found {
			hitCount++
		} else {
			cache.Set(key, fmt.Sprintf("value_%d", key))
		}
	}

	hitRate := float64(hitCount) / float64(totalRequests) * 100
	fmt.Printf("Zipfian 分布访问命中率: %.2f%%\n", hitRate)
}

// CacheHitRateStats 详细的缓存统计信息
// === 详细缓存统计 ===
// 总访问次数: 5000
// 命中次数: 4095
// 未命中次数: 905
// 最终缓存大小: 905
// 缓存命中率: 81.90%
func TestCacheDetailedStats(t *testing.T) {
	cache := NewCache(1000)

	type AccessStat struct {
		hitCount    int
		missCount   int
		totalAccess int
	}

	stats := AccessStat{}

	// 模拟混合访问模式
	for i := 0; i < 5000; i++ {
		var key int
		if i%10 < 8 { // 80% 的访问集中在热数据
			key = rand.Intn(200) // 热数据区域
		} else {
			key = rand.Intn(2000) // 冷数据区域
		}

		_, found := cache.Get(key)
		if found {
			stats.hitCount++
		} else {
			stats.missCount++
			cache.Set(key, fmt.Sprintf("value_%d", key))
		}
		stats.totalAccess++
	}

	hitRate := float64(stats.hitCount) / float64(stats.totalAccess) * 100
	fmt.Printf("\n=== 详细缓存统计 ===\n")
	fmt.Printf("总访问次数: %d\n", stats.totalAccess)
	fmt.Printf("命中次数: %d\n", stats.hitCount)
	fmt.Printf("未命中次数: %d\n", stats.missCount)
	fmt.Printf("最终缓存大小: %d\n", cache.Len())
	fmt.Printf("缓存命中率: %.2f%%\n", hitRate)
	// fmt.Printf("缓存内容: %s\n", cache.String())
}

func TestOptimizedCachePerformance(t *testing.T) {
	// 运行所有测试用例验证优化效果
	//t.Run("TestCacheHitRate", TestCacheHitRate)
	t.Run("TestCacheHitRateRandom", TestCacheHitRateRandom)
	// t.Run("TestCacheHitRateSequential", TestCacheHitRateSequential)
	t.Run("TestCacheHitRateZipfian", TestCacheHitRateZipfian)
	t.Run("TestCacheHitRateWithFixedHotData", TestCacheHitRateWithFixedHotData)
	t.Run("TestCacheHitRateWithDifferentSizes", TestCacheHitRateWithDifferentSizes)
}
