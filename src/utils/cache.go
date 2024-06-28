package utils

type Cache struct {
}

func (c *Cache) Get(key []byte) (interface{}, bool) {
	return nil, false
}

func (c *Cache) Set(key interface{}, value interface{}) bool {
	return false
}
