package lsm

import (
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/interfaces"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/utils"
	"strconv"
	"sync"
	"sync/atomic"
)

type LevelsManger struct {
	maxFID           atomic.Uint64   // sst 已经分配出去的最大fid,只要创建了 MemoryTable 就算已分配;
	levelHandlers    []*levelHandler // 每层的处理器
	opt              *Options
	lsm              *LSM // 上层引用
	txnDoneIndex     atomic.Uint64
	cache            *LevelsCache  // 缓存 block 和 sst.index() 数据
	manifestFile     *ManifestFile // 增删 sst 元信息
	compactIngStatus *compactIngStatus
}

func (lm *LevelsManger) NextFileID() uint64 {
	id := lm.maxFID.Add(1)
	return id
}

func (lsm *LSM) InitLevelManger(opt *Options) *LevelsManger {
	lm := &LevelsManger{
		lsm: lsm,
		opt: opt,
	}
	lm.compactIngStatus = lsm.newCompactStatus()
	if err := lm.loadManifestFile(); err != nil {
		common.Panic(err)
	}
	go lm.getTxnDoneIndexFromCh()
	if err := lm.build(); err != nil {
		common.Panic(err)
	}
	return lm
}

func (lm *LevelsManger) getTxnDoneIndexFromCh() {
	for {
		select {
		case r := <-lm.opt.TxnDoneIndexCh:
			if r != 0 {
				lm.txnDoneIndex.Store(r)
			}
		}
	}
}

func (lm *LevelsManger) getDiscardTs() uint64 {
	return lm.txnDoneIndex.Load()
}

func (lm *LevelsManger) loadManifestFile() (err error) {
	lm.manifestFile, err = OpenManifestFile(&utils.FileOptions{Dir: lm.opt.WorkDir})
	return err
}

func (lm *LevelsManger) build() error {
	lm.levelHandlers = make([]*levelHandler, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levelHandlers[i] = &levelHandler{
			mux:            sync.RWMutex{},
			levelID:        i,
			tables:         make([]*Table, 0),
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
		filePathName := utils.FileNameSSTable(lm.opt.WorkDir, fid)
		if fid > maxFID {
			maxFID = fid
		}
		t, _ := OpenTable(lm, filePathName, nil)
		lm.levelHandlers[tableInfo.LevelID].add(t)
		lm.levelHandlers[tableInfo.LevelID].addSize(t)
	}

	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levelHandlers[i].Sort()
	}

	if maxFID > lm.maxFID.Load() {
		lm.maxFID.Store(maxFID)
	}
	return nil
}

func (lm *LevelsManger) lastLevel() *levelHandler {
	return lm.levelHandlers[len(lm.levelHandlers)-1]
}

func (lm *LevelsManger) iterators(opt *interfaces.Options) []interfaces.Iterator {
	iters := make([]interfaces.Iterator, 0)
	for _, handler := range lm.levelHandlers {
		iters = append(iters, handler.iterators(opt)...)
	}
	return iters
}

func (lm *LevelsManger) Get(keyTs []byte, skipListEntryMaxTs *model.Entry) (model.Entry, error) {
	var (
		entry model.Entry
	)
	startTs := model.ParseTsVersion(keyTs)
	// 对 level-0层的所有table进行搜寻,返回其中最高版本的数据;
	entry, _ = lm.levelHandlers[0].Get(keyTs)
	if entry.Version != 0 {
		if entry.Version == startTs {
			return entry, nil
		}
		if entry.Version > skipListEntryMaxTs.Version {
			skipListEntryMaxTs = &entry
		}
	}
	for i := 1; i < lm.opt.MaxLevelNum; i++ {
		entry, _ = lm.levelHandlers[i].Get(keyTs)
		if entry.Version != 0 {
			if entry.Version == startTs {
				return entry, nil
			}
			if entry.Version > skipListEntryMaxTs.Version {
				skipListEntryMaxTs = &entry
			}
		}
	}
	safeCopy := skipListEntryMaxTs.SafeCopy()
	// 否则到最后, 返回 存储的最高版本的数据;
	return safeCopy, nil
}

func (lm *LevelsManger) checkOverlap(tables []*Table, lev int) bool {
	kr := getKeyRange(tables...) // 给定的 table 区间
	for i, lh := range lm.levelHandlers {
		if i < lev { // 跳过 低于本层的;
			continue
		}
		lh.mux.RLock()
		// 判断当前 level 是否存在区间;
		left, right := lh.findOverLappingTables(levelHandlerRLocked{}, kr)
		lh.mux.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

func (lm *LevelsManger) flush(imm *MemoryTable) (err error) {
	fid := imm.wal.Fid()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, fid)

	builder := NewSSTBuilder(lm.opt)
	skipListIterator := imm.skipList.NewSkipListIterator(strconv.FormatUint(fid, 10) + MemTableName)
	defer skipListIterator.Close() // 涉及到 immemoryTable 的清除和相关 wal 的清理;
	for skipListIterator.Rewind(); skipListIterator.Valid(); skipListIterator.Next() {
		entry := skipListIterator.Item().Item
		builder.Add(&entry, false)
	}

	// 此时磁盘中已经生成 .sst 文件;
	t, _ := OpenTable(lm, sstName, builder)
	// 向manifest 中添加, 添加失败了呢?
	// 假设5.wal 刚转化成 5.sst, 那么5.wal理应被删除掉;但是和5.wal绑定的跳表正在被引用,因此无法直接删除掉5.wal;
	// 随后 系统突然宕机关闭, 重启时, 会先加载 .sst文件, 然后再加载.wal; 那么就会出现 5.wal 和 5.sst 的重叠;
	// 因此, 在 wal 刷盘时, 等于 .sst 的最大文件ID的 .wal 文件需要删除掉;
	err = lm.manifestFile.AddTableMeta(0, &TableMeta{
		ID:       fid,
		Checksum: []byte{'s', 'k', 'i', 'p'},
	})
	common.Panic(err)
	lm.levelHandlers[0].add(t)
	// fmt.Printf("flush sstable %d.sst; \n", fid)
	return nil
}

func (lm *LevelsManger) close() error {
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
