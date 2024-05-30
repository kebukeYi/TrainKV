package lsm

import "trainKv/model"

type levelManger struct {
	maxFID uint64
	levels []*levelHandler
	opt    *Options
}

func InitLevelManger(opt *Options) *levelManger {
	return nil
}

func (receiver *levelManger) Get(key []byte) (*model.Entry, error) {
	return nil, nil
}

type levelHandler struct {
	ssTables []*SSTable
}
