package lsm

import "sync"

type levelHandler struct {
	sync.RWMutex
	levelID        int
	tables         []*table
	totalSize      int64
	totalStaleSize int64
	lm             *levelsManger
}
