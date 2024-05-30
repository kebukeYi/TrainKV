package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"trainKv/model"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList(1000)

	//Put & Get
	entry1 := model.NewEntry([]byte(RandString(10)), []byte("Val1"))
	list.Put(entry1)
	vs := list.Get(entry1.Key)
	assert.Equal(t, entry1.Value, vs.Value)

	entry2 := model.NewEntry([]byte(RandString(10)), []byte("Val2"))
	list.Put(entry2)
	vs = list.Get(entry2.Key)
	assert.Equal(t, entry2.Value, vs.Value)

	//Get a not exist entry
	assert.Nil(t, list.Get([]byte(RandString(10))).Value)

	//Update a entry
	entry2_new := model.NewEntry(entry1.Key, []byte("Val1+1"))
	list.Put(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Get(entry2_new.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList(100000000)
	key, val := "", ""
	maxTime := 1000
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = RandString(10), fmt.Sprintf("Val%d", i)
		entry := model.NewEntry([]byte(key), []byte(val))
		list.Put(entry)
		searchVal := list.Get([]byte(key)).Value
		assert.Equal(b, searchVal, []byte(val))
	}
}

func TestDrawList(t *testing.T) {
	list := NewSkipList(1000)
	n := 12
	for i := 0; i < n; i++ {
		index := strconv.Itoa(r.Intn(90) + 10)
		key := index + RandString(8)
		entryRand := model.NewEntry([]byte(key), []byte(index))
		list.Put(entryRand)
	}
	list.Draw(true)
	fmt.Println(strings.Repeat("*", 30) + "分割线" + strings.Repeat("*", 30))
	list.Draw(false)
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("Keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Put(model.NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Get(key(i)).Value
			require.EqualValues(t, key(i), v)
			return

			require.Nil(t, v)
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Put(model.NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Get(key(i)).Value
			require.EqualValues(b, key(i), v)
			require.NotNil(b, v)
		}(i)
	}
	wg.Wait()
}

func TestSkipListIterator(t *testing.T) {
	list := NewSkipList(100000)

	//Put & Get
	entry1 := model.NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Put(entry1)
	assert.Equal(t, entry1.Value, list.Get(entry1.Key).Value)

	entry2 := model.NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Put(entry2)
	assert.Equal(t, entry2.Value, list.Get(entry2.Key).Value)

	//Update a entry
	entry2_new := model.NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Put(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Get(entry2_new.Key).Value)

	iter := list.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		fmt.Printf("iter key %s, value %s", iter.Item().Item.Key, iter.Item().Item.Value)
	}
}
