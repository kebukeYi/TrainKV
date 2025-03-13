package cache

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCacheLRUCRUD(t *testing.T) {
	numEntries := 10
	cache := NewCache(numEntries)
	// winLRU  10%
	// sLRUOne 30%
	// sLRUTwo 70%
	basic := 15
	for i := 0; i < basic; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		cache.Set(key, val)
		fmt.Printf("set key:%s; cache:%v\n", key, cache)
	}

	for i := 0; i < basic; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, ok := cache.Get(key)
		if ok {
			fmt.Printf("get key:%s; cache:%s\n", key, cache)
			assert.Equal(t, val, res)
			continue
		} else {
			fmt.Printf("get key:%s; not found!", key)
		}
	}
	fmt.Printf("at last cache: %s\n", cache)
}
