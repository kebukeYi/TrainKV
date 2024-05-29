package lsm

import (
	"sync"
	. "trainKv/utils"
)

type memoryTable struct {
	mux      *sync.Mutex
	skipList *SkipList
	wal      *WAL
}

func NewMemoryTable() *memoryTable {
	return &memoryTable{}
}

func (t memoryTable) Get(key []byte) []byte {
	return nil
}

func (t memoryTable) Put(key, value []byte) bool {
	return false
}
