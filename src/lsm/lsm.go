package lsm

import (
	"trainKv/common"
)

type LSM struct {
	memoryTable   *memoryTable
	imemoryTables []*memoryTable
	levelManger   *levelManger
}

func NewLSM() *LSM {
	return &LSM{}
}
func (lsm *LSM) Get(key []byte) ([]byte, error) {
	get := lsm.memoryTable.Get(key)
	if get != nil {
		return get, nil
	}
	for _, table := range lsm.imemoryTables {
		if get = table.Get(key); get != nil {
			return get, nil
		}
	}
	return nil, common.ErrNotFound
}

func (lsm *LSM) Put(key, value []byte) bool {
	return lsm.memoryTable.Put(key, value)
}
