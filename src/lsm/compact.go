package lsm

import "sync"

// keyRange
type keyRange struct {
	left  []byte
	right []byte
	inf   bool  // 是否合并过
	size  int64 // size is used for Key splits.
}

// levelCompactStatus
type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

type compactIngStatus struct {
	mux    sync.Mutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

func (lsm LSM) newCompactStatus() *compactIngStatus {
	cs := &compactIngStatus{
		mux:    sync.Mutex{},
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < lsm.option.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}
