package lsm

import (
	"trainKv/model"
	"trainKv/utils"
)

type levelsManger struct {
	maxFID        uint64
	levelHandlers []*levelHandler
	opt           *Options
	lsm           *LSM
	cache         *utils.Cache
	manifestFile  *ManifestFile
}

func InitLevelManger(opt *Options) *levelsManger {
	lm := &levelsManger{
		maxFID:        0,
		levelHandlers: make([]*levelHandler, 0),
		opt:           opt,
	}
	return lm
}

func (receiver *levelsManger) Get(key []byte) (*model.Entry, error) {
	return nil, nil
}
