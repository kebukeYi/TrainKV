package lsm

import (
	"strconv"
	"sync"
	"sync/atomic"
	"trainKv/common"
	"trainKv/model"
	"trainKv/utils"
)

type levelsManger struct {
	maxFID        uint64          // 已经分配出去的最大fid，只要创建了 memoryTable 就算已分配
	levelHandlers []*levelHandler // 每层的处理器
	opt           *Options
	lsm           *LSM // 上层引用
	//cache            *cache.Cache  // 缓存 block 和 sst.index() 数据
	cache            *LevelsCache  // 缓存 block 和 sst.index() 数据
	manifestFile     *ManifestFile // 增删 sst 元信息
	compactIngStatus *compactIngStatus
}

func (lsm *LSM) InitLevelManger(opt *Options) *levelsManger {
	lm := &levelsManger{
		lsm:    lsm,
		maxFID: 0,
		opt:    opt,
	}
	lm.compactIngStatus = lsm.newCompactStatus()
	if err := lm.loadManifestFile(); err != nil {
		common.Panic(err)
	}
	if err := lm.build(); err != nil {
		common.Panic(err)
	}
	return lm
}

func (lm *levelsManger) loadManifestFile() (err error) {
	// 打开的同时, 并做好了内存数据结构;
	lm.manifestFile, err = OpenManifestFile(&model.FileOptions{Dir: lm.opt.WorkDir})
	return err
}

func (lm *levelsManger) build() error {
	lm.levelHandlers = make([]*levelHandler, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levelHandlers[i] = &levelHandler{
			mux:            sync.RWMutex{},
			levelID:        i,
			tables:         make([]*table, 0),
			totalSize:      0,
			totalStaleSize: 0,
			lm:             lm,
		}
	}

	manifest := lm.manifestFile.GetManifest()
	if err := lm.manifestFile.checkSSTable(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}

	lm.cache = newLevelsCache(lm.opt)

	var maxFID uint64
	for fid, tableInfo := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fid)
		if fid > maxFID {
			maxFID = fid
		}
		t := openTable(lm, fileName, nil)
		lm.levelHandlers[tableInfo.LevelID].add(t)
		lm.levelHandlers[tableInfo.LevelID].addSize(t)
	}

	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levelHandlers[i].Sort()
	}
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
}

func (lm *levelsManger) lastLevel() *levelHandler {
	return lm.levelHandlers[len(lm.levelHandlers)-1]
}
func (lm *levelsManger) iterators(opt *model.Options) []model.Iterator {
	iters := make([]model.Iterator, 0)
	for _, handler := range lm.levelHandlers {
		iters = append(iters, handler.iterators(opt)...)
	}
	return iters
}

func (lm *levelsManger) Get(key []byte) (*model.Entry, error) {
	var (
		entry *model.Entry
		err   error
	)
	// L0层查询
	if entry, err = lm.levelHandlers[0].Get(key); entry != nil {
		return entry, err
	}
	for i := 1; i < lm.opt.MaxLevelNum; i++ {
		if entry, err = lm.levelHandlers[i].Get(key); entry != nil {
			return entry, nil
		}
	}
	return entry, common.ErrKeyNotFound
}

func (lm *levelsManger) flush(imm *memoryTable) (err error) {
	fid := imm.wal.Fid()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, fid)

	builder := newSSTBuilder(lm.opt)
	skipListIterator := imm.skipList.NewSkipListIterator(strconv.FormatUint(fid, 10) + MemTableName)
	for skipListIterator.Rewind(); skipListIterator.Valid(); skipListIterator.Next() {
		entry := skipListIterator.Item().Item
		builder.add(entry, false)
	}

	t := openTable(lm, sstName, builder)
	err = lm.manifestFile.AddTableMeta(0, &TableMeta{
		ID:       fid,
		Checksum: []byte{'s', 'k', 'i', 'p'},
	})
	common.Panic(err)
	lm.levelHandlers[0].add(t)
	return nil
}
func (lm *levelsManger) close() error {
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	for i := range lm.levelHandlers {
		if err := lm.levelHandlers[i].close(); err != nil {
			return err
		}
	}
	return nil
}
