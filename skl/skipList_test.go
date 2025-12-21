package skl

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func TestSkipList_Put(t *testing.T) {
	skipList := NewSkipList(3000)
	putStart := 0
	putEnd := 90
	fmt.Println("========================put(0-60)==================================")
	// 写入 0-60
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		key = string(model.KeyWithTs([]byte(key), 1))
		val := fmt.Sprintf("val%d", i)
		e := model.NewEntry([]byte(key), []byte(val))
		e.ExpiresAt = uint64(i)
		skipList.Put(e)
	}
	fmt.Printf("num: %d\n", atomic.LoadInt32(&skipList.num))
	fmt.Println("========================get(0-90)==================================")
	// 读取
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		key = string(model.KeyWithTs([]byte(key), 1))
		// 查询
		valueExt := skipList.Get([]byte(key))
		if valueExt.ExpiresAt != 0 {
			fmt.Printf("key: %s, value: %s, exp:%d \n", key, valueExt.Value, valueExt.ExpiresAt)
		}
	}
}

func TestSkipListUpdate(t *testing.T) {
	list := NewSkipList(1000)
	for i := 0; i < 20; i++ {
		list.Put(model.NewEntry([]byte(RandString(10)), []byte(RandString(10))))
	}
	//Put & get
	key := fmt.Sprintf("key%d", 60)
	val := fmt.Sprintf("val%d", 60)
	e := model.NewEntry([]byte(key), []byte(val))
	e.Key = model.KeyWithTs(e.Key, 1)
	list.Put(e)
	vs := list.Get(e.Key)
	assert.Equal(t, e.Value, vs.Value)

	// update1
	e.Value = []byte("val64")
	list.Put(e)

	// update2
	time.Sleep(100 * time.Millisecond)
	e.Key = model.KeyWithTs([]byte(key), 1)
	e.Value = []byte("val68")
	list.Put(e)

	// random put
	entry1 := model.NewEntry([]byte(RandString(10)), []byte("Val1"))
	list.Put(entry1)
	vs = list.Get(entry1.Key)
	assert.Equal(t, entry1.Value, vs.Value)

	entry2 := model.NewEntry([]byte(RandString(10)), []byte("Val2"))
	list.Put(entry2)
	vs = list.Get(entry2.Key)
	assert.Equal(t, entry2.Value, vs.Value)

	//get a not exist entry.
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
		key = fmt.Sprintf("key%d", i)
		val = fmt.Sprintf("Val%d", i)

		keyTs := string(model.KeyWithTs([]byte(key), 1))
		entry := model.NewEntry([]byte(keyTs), []byte(val))
		list.Put(entry)

		keyTs = string(model.KeyWithTs([]byte(key), 1))
		searchVal := list.Get([]byte(keyTs)).Value
		//fmt.Printf("key: %s, value: %s \n", key, searchVal)
		assert.Equal(b, searchVal, []byte(val))
	}
	fmt.Printf("num: %d\n", atomic.LoadInt32(&list.num))
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
			l.Put(model.NewEntry(model.KeyWithTs(key(i), 1), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Get(model.KeyWithTs(key(i), 1)).Value
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
			l.Put(model.NewEntry(model.KeyWithTs(key(i), 1), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Get(model.KeyWithTs(key(i), 1)).Value
			require.EqualValues(b, key(i), v)
			require.NotNil(b, v)
		}(i)
	}
	wg.Wait()
}

func TestSkipListIterator(t *testing.T) {
	list := NewSkipList(100000)

	//Put & get
	entry1 := model.NewEntry(model.KeyWithTs([]byte(RandString(10)), 1), []byte(RandString(10)))
	list.Put(entry1)
	assert.Equal(t, entry1.Value, list.Get(entry1.Key).Value)

	entry2 := model.NewEntry(model.KeyWithTs([]byte(RandString(10)), 1), []byte(RandString(10)))
	list.Put(entry2)
	assert.Equal(t, entry2.Value, list.Get(entry2.Key).Value)

	//Update a entry
	entry2_new := model.NewEntry(entry2.Key, []byte(RandString(10)))
	list.Put(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Get(entry2_new.Key).Value)

	iter := list.NewSkipListIterator("1")
	for iter.Rewind(); iter.Valid(); iter.Next() {
		fmt.Printf("iter key %s, value %s\n ", model.ParseKey(iter.Item().Item.Key), iter.Item().Item.Value)
	}
}

func randomHeight() int {
	h := 1
	for h < maxHeight {
		rand := rand.Uint32()
		fmt.Println("rand: ", rand)
		fastRand := model.FastRand()
		if fastRand <= heightIncrease {
			h++
		}
	}
	return h
}

func TestRandomHeight(t *testing.T) {
	height := randomHeight()
	fmt.Println(height)
}

func TestDrawList(t *testing.T) {
	list := NewSkipList(1000)
	n := 12
	for i := 0; i < n; i++ {
		index := strconv.Itoa(r.Intn(90) + 10)
		key := index + RandString(8)
		entryRand := model.NewEntry(model.KeyWithTs([]byte(key), 1), []byte(index))
		list.Put(entryRand)
	}
	list.Draw(true)
	fmt.Println(strings.Repeat("*", 30) + "分割线" + strings.Repeat("*", 30))
	list.Draw(false)
}
