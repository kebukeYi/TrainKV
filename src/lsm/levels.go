package lsm

import "trainKv/model"

type levelManger struct {
	maxFID uint64
	levels []*levelHandler
	opt    *Options
}

func InitLevelManger(opt *Options) *levelManger {
	lm := &levelManger{
		maxFID: 0,
		levels: make([]*levelHandler, 0),
		opt:    opt,
	}
	return lm
}

func (receiver *levelManger) Get(key []byte) (*model.Entry, error) {
	return nil, nil
}

type levelHandler struct {
	ssTables []*SSTable
}
