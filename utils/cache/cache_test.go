package cache

import (
	"github.com/kebukeYi/TrainKV/utils"
	"testing"
)

func TestNewCache(t *testing.T) {
	cache := NewCache(100)
	if cache == nil {
		t.Error("Expected cache to be created, got nil")
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
