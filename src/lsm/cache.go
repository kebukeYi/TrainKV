package lsm

import "trainKv/utils/cache"

type LevelsCache struct {
	indexData *cache.Cache
	blockData *cache.Cache
}

const defaultCacheSize = 1024 * 10

func newLevelsCache(opt *Options) *LevelsCache {
	if opt.CacheSize == 0 {
		opt.CacheSize = defaultCacheSize
	}
	return &LevelsCache{
		indexData: cache.NewCache(opt.CacheSize),
		blockData: cache.NewCache(opt.CacheSize),
	}
}
