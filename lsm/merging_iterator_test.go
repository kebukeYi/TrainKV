package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/stretchr/testify/assert"
	"sort"
	"strings"
	"testing"
)

// mockIterator 是一个模拟的迭代器实现，用于测试 MergingIterator;
type mockIterator struct {
	name    string
	entries []model.Entry
	index   int
	isAsc   bool
	seekKey []byte
}

func newMockIterator(name string, entries []model.Entry, isAsc bool) *mockIterator {
	sort.Slice(entries, func(i, j int) bool {
		return model.CompareKeyWithTs(entries[i].Key, entries[j].Key) < 0
	})
	return &mockIterator{
		name:    name,
		entries: entries,
		index:   -1,
		isAsc:   isAsc,
	}
}

func (m *mockIterator) Name() string {
	return m.name
}

func (m *mockIterator) Next() {
	if m.isAsc {
		m.index++
	} else {
		m.index--
	}
}

func (m *mockIterator) Valid() bool {
	return m.index >= 0 && m.index < len(m.entries)
}

func (m *mockIterator) Rewind() {
	if m.isAsc {
		m.index = 0
	} else {
		m.index = len(m.entries) - 1
	}
}

func (m *mockIterator) Item() interfaces.Item {
	if !m.Valid() {
		panic("Invalid iterator")
	}
	return interfaces.Item{Item: m.entries[m.index]}
}

func (m *mockIterator) Seek(key []byte) {
	m.seekKey = key
	// 简单实现：找到第一个大于等于key的项
	for i, entry := range m.entries {
		if model.CompareKeyWithTs(entry.Key, key) >= 0 {
			m.index = i
			return
		}
	}
	// 如果没找到，则设置为无效
	m.index = len(m.entries)
}

func (m *mockIterator) Close() error {
	return nil
}

// 创建测试数据
func createTestEntries(keys []string, versions []uint64) []model.Entry {
	entries := make([]model.Entry, 0, len(keys))
	for i, key := range keys {
		version := uint64(1)
		if i < len(versions) {
			version = versions[i]
		}
		keyWithTs := model.KeyWithTs([]byte(key), version)
		entry := model.Entry{
			Key:   keyWithTs,
			Value: []byte(fmt.Sprintf("value_%s_%d", key, version)),
		}
		entries = append(entries, entry)
	}
	return entries
}

func TestMergingIteratorBasic(t *testing.T) {
	// 创建测试数据  tx := 10 - version;
	// sst_1: boo8, foo2, foo3, zoo1, zoo2;
	entries1 := createTestEntries([]string{"boo", "foo", "foo", "zoo", "zoo"}, []uint64{2, 7, 8, 8, 9})

	// sst_2: foo5, foo6, foo9, zoo9, zoo5;
	entries2 := createTestEntries([]string{"foo", "foo", "foo", "zoo", "zoo"}, []uint64{1, 4, 5, 1, 5})

	// sst_3: aoo9, boo7, coo2, poo7, poo8;
	entries3 := createTestEntries([]string{"aoo", "boo", "coo", "poo", "poo"}, []uint64{1, 3, 8, 2, 3})

	// 创建模拟迭代器
	iter1 := newMockIterator("mock1", entries1, true)
	iter2 := newMockIterator("mock2", entries2, true)
	iter3 := newMockIterator("mock3", entries3, true)

	// 创建 MergingIterator
	iters := []interfaces.Iterator{iter1, iter2, iter3}
	mergingIter := NewMergingIterator(iters, &interfaces.Options{IsAsc: true})

	// 测试 Rewind
	mergingIter.Rewind()

	// 验证合并结果是否正确
	// 注意: 不同key按照字典排序, 相同key按照版本降序排序;(越小,说明版本号越高);
	// 正确合并结果应为: aoo1, boo3, boo2, coo8, foo8, foo7, foo5, foo4, foo1, poo3, poo2, zoo9, zoo8, zoo5, zoo1
	expectedKeys := []string{"aoo", "boo", "boo", "coo", "foo", "foo", "foo", "foo", "foo", "poo", "poo", "zoo", "zoo", "zoo", "zoo"}
	expectedVersions := []uint64{1, 3, 2, 8, 8, 7, 5, 4, 1, 3, 2, 9, 8, 5, 1}

	i := 0
	for ; mergingIter.Valid(); mergingIter.Next() {
		item := mergingIter.Item()
		key := model.ParseKey(item.Item.Key)
		version := model.ParseTsVersion(item.Item.Key)

		assert.True(t, i < len(expectedKeys), "More items than expected")
		assert.Equal(t, expectedKeys[i], string(key), "Key mismatch at index %d", i)
		assert.Equal(t, expectedVersions[i], version, "Version mismatch at index %d", i)

		fmt.Printf("Key: %s, Version: %d, Value: %s\n", key, version, item.Item.Value)
		i++
	}

	assert.Equal(t, len(expectedKeys), i, "Number of items mismatch")
}

func TestMergingIteratorEmpty(t *testing.T) {
	// 测试空迭代器情况
	iters := []interfaces.Iterator{}
	mergingIter := NewMergingIterator(iters, &interfaces.Options{IsAsc: true})

	// 空迭代器应该无效
	assert.False(t, mergingIter.Valid())
}

func TestMergingIteratorSingle(t *testing.T) {
	// 测试只有一个迭代器的情况
	entries := createTestEntries([]string{"a", "b", "c"}, []uint64{1, 1, 1})
	iter := newMockIterator("single", entries, true)
	iters := []interfaces.Iterator{iter}
	mergingIter := NewMergingIterator(iters, &interfaces.Options{IsAsc: true})

	mergingIter.Rewind()

	expectedKeys := []string{"a", "b", "c"}
	i := 0
	for ; mergingIter.Valid(); mergingIter.Next() {
		item := mergingIter.Item()
		key := model.ParseKey(item.Item.Key)

		assert.Equal(t, expectedKeys[i], string(key))
		i++
	}
	assert.Equal(t, 3, i)
}

func TestMergingIteratorSeek(t *testing.T) {
	// 测试 Seek 功能
	entries1 := createTestEntries([]string{"a", "c", "e"}, []uint64{1, 1, 1})
	entries2 := createTestEntries([]string{"b", "d", "f"}, []uint64{1, 1, 1})

	iter1 := newMockIterator("seek1", entries1, true)
	iter2 := newMockIterator("seek2", entries2, true)

	iters := []interfaces.Iterator{iter1, iter2}
	mergingIter := NewMergingIterator(iters, &interfaces.Options{IsAsc: true})

	// Seek 到 "c"
	seekKey := model.KeyWithTs([]byte("c"), 1)
	mergingIter.Seek(seekKey)

	// 应该定位到 "c"
	assert.True(t, mergingIter.Valid())
	if mergingIter.Valid() {
		item := mergingIter.Item()
		key := model.ParseKey(item.Item.Key)
		assert.Equal(t, "c", string(key))
	}
	for ; mergingIter.Valid(); mergingIter.Next() {
		item := mergingIter.Item()
		key := model.ParseKey(item.Item.Key)
		assert.True(t, strings.Compare(string(key), "c") >= 0)
		fmt.Printf("Key: %s, Value: %s\n", key, item.Item.Value)
	}
}

func TestMergingIteratorClose(t *testing.T) {
	// 测试 Close 功能
	entries := createTestEntries([]string{"a", "b"}, []uint64{1, 1})
	iter := newMockIterator("close_test", entries, true)
	iters := []interfaces.Iterator{iter}
	mergingIter := NewMergingIterator(iters, &interfaces.Options{IsAsc: true})

	// 关闭应该没有错误;
	err := mergingIter.Close()
	assert.NoError(t, err)
}
