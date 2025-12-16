package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/pb"
	"github.com/kebukeYi/TrainKV/utils"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

type compactionPriority struct {
	levelId  int
	score    float64
	adjusted float64
	dst      targets
}

type targets struct {
	dstLevelId       int
	levelTargetSSize []int64 // 对应层中所有 .sst 文件的期望总大小; 用于计算 每层的优先级;
	fileSize         []int64 // 对应层中单个 .sst 文件的期望大小; 用于设定 合并的生成的目标 sst 文件大小;
}

type compactDef struct {
	compactorId int
	prior       compactionPriority
	dst         targets
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	thisTables []*Table
	nextTables []*Table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64
}

type thisAndNextLevelRLocked struct{}

func (cd *compactDef) lockLevel() {
	cd.thisLevel.mux.RLock()
	cd.nextLevel.mux.RLock()
}

func (cd *compactDef) unlockLevel() {
	cd.thisLevel.mux.RUnlock()
	cd.nextLevel.mux.RUnlock()
}

// 1. 启动压缩
func (lm *LevelsManger) runCompacter(compactorId int, closer *utils.Closer) {
	defer closer.Done()
	randomDelay := time.NewTimer(time.Duration(rand.Intn(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-closer.CloseSignal:
		randomDelay.Stop()
		return
	}
	ticker := time.NewTicker(50000 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			lm.runOnce(compactorId)
			// fmt.Printf("[compactorId:%d] Compaction start.\n", compactorId)
		case <-closer.CloseSignal:
			ticker.Stop()
			return
		}
	}
}

func (lm *LevelsManger) runOnce(compactorId int) bool {
	prios := lm.pickCompactLevels()
	if compactorId == 0 {
		prios = moveL0toFront(prios)
	}
	for _, prio := range prios {
		if prio.levelId == 0 && compactorId == 0 {

		} else if prio.adjusted < 1.0 {
			break
		}
		if lm.run(compactorId, prio) {
			return true
		}
	}
	return false
}

func moveL0toFront(prios []compactionPriority) []compactionPriority {
	idx := -1
	for i, prio := range prios {
		if prio.levelId == 1 {
			idx = i
			break
		}
	}
	if idx > 0 {
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

func (lm *LevelsManger) run(compactorId int, prio compactionPriority) bool {
	// id是协程id, for:p 是将要参与合并的 X层源头层级, 此时目标 Y层 已经找好了;
	// X层: l0层
	// X层: ln层
	err := lm.doCompact(compactorId, prio)
	switch err {
	case nil:
		return true
	case common.ErrFillTables:
	default:
		log.Printf("[taskID:%d] While running doCompact: %v\\n.", compactorId, err)
	}
	return false
}

// 2. 寻找压缩目的地 Y(nextLevel) 层
func (lm *LevelsManger) levelTargets() targets {
	adjusted := func(sz int64) int64 {
		if sz < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return sz
	}
	dst := targets{
		levelTargetSSize: make([]int64, len(lm.levelHandlers)),
		fileSize:         make([]int64, len(lm.levelHandlers)),
	}
	totalSize := lm.lastLevel().getTotalSize()
	// 从下层向上递减;
	for i := len(lm.levelHandlers) - 1; i > 0; i-- {
		levelTargetSize := adjusted(totalSize)
		dst.levelTargetSSize[i] = levelTargetSize
		if dst.dstLevelId == 0 && levelTargetSize <= lm.opt.BaseLevelSize {
			dst.dstLevelId = i
		}
		//        | |        BaseLevelSize
		//        | |        BaseLevelSize
		//       /    \      totalSize/100
		//     /        \    totalSize/10
		//   /            \  totalSize
		totalSize = totalSize / int64(lm.opt.LevelSizeMultiplier)
	}

	baseTableSize := lm.opt.BaseTableSize
	// 从上往下递增; 计算文件大小的目的是, 可设定在合并时 生成的 sst 文件大小;
	for i := 0; i < len(lm.levelHandlers); i++ {
		if i == 0 {
			dst.fileSize[i] = lm.opt.MemTableSize
		} else if i <= dst.dstLevelId {
			// 小于等于 Y 目标层的文件 都是一致的,形成下面的形状;
			//    ||    MemTableSize
			//   |  |   BaseTableSize
			//   |  |   BaseTableSize
			//  /    \  BaseTableSize * TableSizeMultiplier
			dst.fileSize[i] = baseTableSize
		} else {
			baseTableSize *= int64(lm.opt.TableSizeMultiplier)
			dst.fileSize[i] = baseTableSize
		}
	}

	for i := dst.dstLevelId + 1; i < len(lm.levelHandlers)-1; i++ {
		if lm.levelHandlers[i].getTotalSize() > 0 {
			break
		}
		dst.dstLevelId = i
	}

	dstLevelId := dst.dstLevelId
	lvl := lm.levelHandlers
	if dstLevelId < len(lvl)-1 && lvl[dstLevelId].getTotalSize() == 0 &&
		lvl[dstLevelId+1].getTotalSize() < dst.fileSize[dstLevelId+1] {
		dst.dstLevelId++
	}
	return dst
}

// 3. 为每一层创建一个压缩信息
func (lm *LevelsManger) pickCompactLevels() (prios []compactionPriority) {
	levelTargets := lm.levelTargets()
	addPriority := func(level int, score float64) {
		prio := compactionPriority{
			levelId:  level,
			score:    score,
			adjusted: score,
			dst:      levelTargets,
		}
		prios = append(prios, prio)
	}

	addPriority(0, float64(lm.levelHandlers[0].numTables()/lm.opt.NumLevelZeroTables))

	for i := 1; i < len(lm.levelHandlers); i++ {
		delSize := lm.compactIngStatus.getLevelDelSize(i)
		size := lm.levelHandlers[i].getTotalSize() - delSize
		addPriority(i, float64(size/levelTargets.levelTargetSSize[i]))
	}

	common.CondPanic(len(prios) != len(lm.levelHandlers),
		errors.New("[pickCompactLevels] len(prios) != len(lm.levels)"))

	var preLevel int
	for level := levelTargets.dstLevelId; level < len(lm.levelHandlers); level++ {
		if prios[preLevel].adjusted > 1.0 {
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[preLevel].adjusted = prios[preLevel].adjusted / prios[level].adjusted
			} else {
				prios[preLevel].adjusted = prios[preLevel].adjusted / minScore
			}
		}
		preLevel = level
	}

	out := prios[:0]
	for _, prio := range prios {
		if prio.score >= 1.0 {
			out = append(out, prio)
		}
	}

	prios = out
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})

	return prios
}

// 4. 开始遍历寻找 X层 中适合参与压缩的table, l0 -> lY ; l0->l0; lmax->lmax; lx -> lx+1;
func (lm *LevelsManger) doCompact(id int, prio compactionPriority) error {
	if prio.dst.dstLevelId == 0 {
		// 重新统计一次 现有所有 .sst 文件大小情况, 以及期望大小;
		prio.dst = lm.levelTargets()
	}

	cd := compactDef{
		compactorId: id,
		prior:       prio,
		dst:         prio.dst,
		thisLevel:   lm.levelHandlers[prio.levelId],
	}

	if prio.levelId == 0 {
		cd.nextLevel = lm.levelHandlers[prio.dst.dstLevelId]
		if !lm.findTablesL0(&cd) {
			return common.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = lm.levelHandlers[prio.levelId+1]
		}
		if !lm.findTables(&cd) {
			return common.ErrFillTables
		}
	}

	defer lm.compactIngStatus.deleteCompactionDef(cd)

	if err := lm.runCompactDef(id, prio.dst.dstLevelId, cd); err != nil {
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}
	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelID)

	return nil
}

// 4.1. l0 -> ly
// 4.2. l0 -> l0
func (lm *LevelsManger) findTablesL0(cd *compactDef) bool {
	if ok := lm.findTablesL0ToDstLevel(cd); ok {
		return true
	}
	return lm.findTablesL0ToL0(cd)
}

func (lm *LevelsManger) findTablesL0ToDstLevel(cd *compactDef) bool {
	if cd.prior.adjusted > 0.0 && cd.prior.adjusted < 1.0 {
		return false
	}
	cd.lockLevel()
	defer cd.unlockLevel()
	xTables := cd.thisLevel.tables
	if len(xTables) == 0 {
		return false
	}

	var out []*Table
	var kr keyRange
	for _, t := range xTables {
		tkr := getKeyRange(t)
		if kr.overlapWith(tkr) {
			out = append(out, t)
			kr.extend(tkr)
		} else {
			break
		}
	}
	cd.thisRange = getKeyRange(out...)
	cd.thisTables = out

	left, right := cd.nextLevel.findOverLappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.nextTables = make([]*Table, right-left)
	copy(cd.nextTables, cd.nextLevel.tables[left:right])

	if len(cd.nextTables) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.nextTables...)
	}

	return lm.compactIngStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

func (lm *LevelsManger) findTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		return false
	}
	cd.nextLevel = lm.levelHandlers[0]
	cd.nextRange = keyRange{}
	cd.nextTables = nil

	common.CondPanic(cd.thisLevel.levelID != 0, errors.New("fillTablesL0ToL0 cd.thisLevel.levelNum != 0"))
	common.CondPanic(cd.nextLevel.levelID != 0, errors.New("fillTablesL0ToL0 cd.nextLevel.levelNum != 0"))

	lm.levelHandlers[0].mux.RLock()
	defer lm.levelHandlers[0].mux.RUnlock()

	lm.compactIngStatus.mux.Lock()
	defer lm.compactIngStatus.mux.Unlock()

	top := cd.thisLevel.tables
	var out []*Table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.dst.fileSize[0] {
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			continue
		}
		if _, ok := lm.compactIngStatus.tables[t.fid]; ok {
			continue
		}
		out = append(out, t)
	}
	if len(out) < 4 {
		return false
	}

	cd.thisRange = keyRange{inf: true}
	cd.thisTables = out

	compactStatus := lm.compactIngStatus.levels[cd.thisLevel.levelID]
	compactStatus.ranges = append(compactStatus.ranges, cd.thisRange)

	for _, t := range out {
		lm.compactIngStatus.tables[t.fid] = struct{}{}
	}

	cd.dst.fileSize[0] = math.MaxUint32
	return true
}

// 4.3. lmax -> lmax
// 4.4. lx -> lx+1
func (lm *LevelsManger) findTables(cd *compactDef) bool {
	cd.lockLevel()
	defer cd.unlockLevel()
	tables := make([]*Table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}

	if cd.thisLevel.isLastLevel() {
		return lm.findMaxLevelTables(tables, cd)
	}

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		if lm.compactIngStatus.overlapsWith(cd.thisLevel.levelID, cd.thisRange) {
			continue
		}
		cd.thisTables = []*Table{t}

		// 查询目标层中是否含有与当前层的table存在重叠;
		left, right := cd.nextLevel.findOverLappingTables(levelHandlerRLocked{}, cd.thisRange)
		cd.nextTables = make([]*Table, right-left)
		copy(cd.nextTables, cd.nextLevel.tables[left:right])

		if len(cd.nextTables) == 0 {
			cd.nextTables = []*Table{}
			cd.nextRange = cd.thisRange
			if !lm.compactIngStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				// cas 失败代表: 说明当前表参与合并了, 跳过当前t;
				continue
			}
			// cas 成功代表:说明当前表没有参与合并,但是为什么直接返回了呢?
			// 答: 当下一层没有重叠的表时，这个表可以被移动到下一层中;
			return true
		}

		cd.nextRange = getKeyRange(cd.nextTables...)
		if lm.compactIngStatus.overlapsWith(cd.nextLevel.levelID, cd.nextRange) {
			continue
		}

		if !lm.compactIngStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

func (lm *LevelsManger) findMaxLevelTables(tables []*Table, cd *compactDef) bool {
	sortedTables := make([]*Table, len(tables))
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(tables, cd)
	if len(sortedTables) > 0 && sortedTables[0].getStaleDataSize() == 0 {
		return false
	}

	cd.nextTables = []*Table{}
	collectNextTables := func(t *Table, needSz int64) {
		idx := sort.Search(len(tables), func(i int) bool {
			return model.CompareKeyNoTs(tables[i].sst.minKey, t.sst.minKey) >= 0
		})
		common.CondPanic(tables[idx].fid != t.fid, errors.New("tables[j].ID() != t.ID()"))
		totalSize := t.Size()
		idx++
		for idx < len(tables) {
			totalSize += tables[idx].Size()
			if totalSize >= needSz {
				break
			}
			cd.nextTables = append(cd.nextTables, tables[idx])
			cd.nextRange.extend(getKeyRange(tables[idx]))
			idx++
		}
	}

	now := time.Now()
	for _, t := range sortedTables {
		if now.Sub(*t.GetCreatedAt()) < time.Hour {
			continue
		}
		if t.getStaleDataSize() < uint32(lm.opt.ValueLogFileSize) {
			continue
		}
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)

		cd.nextRange = cd.thisRange

		if lm.compactIngStatus.overlapsWith(cd.thisLevel.levelID, cd.thisRange) {
			continue
		}

		cd.thisTables = []*Table{t}

		needFileSize := cd.dst.fileSize[cd.thisLevel.levelID]
		if t.Size() >= needFileSize {
			break
		}
		collectNextTables(t, needFileSize)
		if !lm.compactIngStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.nextTables = cd.nextTables[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}

	if len(cd.thisTables) == 0 {
		return false
	}
	return lm.compactIngStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

func (lm *LevelsManger) sortByStaleDataSize(tables []*Table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].getStaleDataSize() > tables[j].getStaleDataSize()
	})
}

func (lm *LevelsManger) runCompactDef(id int, level int, cd compactDef) error {
	if len(cd.dst.fileSize) == 0 {
		return errors.New("#runCompactDef() FileSizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()
	common.CondPanic(len(cd.splits) != 0, errors.New("runCompactDef, len(cd.splits) != 0"))
	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel
	if thisLevel == nextLevel {
	} else {
		lm.addSplits(&cd)
	}

	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	buildTables, decrTables, err := lm.compactBuildTables(level, cd)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := decrTables(); err2 == nil {
			err = err2
		}
	}()

	manifestChangeSet := buildChangeSet(&cd, buildTables)

	if err := lm.manifestFile.AddChanges(manifestChangeSet.Changes); err != nil {
		return err
	}

	// bot层, Y目标层表;
	if err = nextLevel.updateTable(cd.nextTables, buildTables); err != nil {
		return err
	}

	// top层, X源头层表;
	if err = thisLevel.deleteTable(cd.thisTables); err != nil {
		return err
	}

	from := append(tablesToString(cd.thisTables), tablesToString(cd.nextTables)...)

	to := tablesToString(buildTables)

	if dur := time.Since(timeStart); dur >= 1*time.Second {
		fmt.Printf("[GoRouteid:%d] Compact Input: lx:%d[%d tables] + ly:%d[%d tables]  with %d splits. -> Out: ly:%d[new %d tables]. tableName: [%s] -> [%s], took %v\n",
			id, thisLevel.levelID, len(cd.thisTables),
			nextLevel.levelID, len(cd.nextTables), len(cd.splits),
			nextLevel.levelID, len(buildTables),
			strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}
	return nil
}

func (lm *LevelsManger) compactBuildTables(level int, cd compactDef) ([]*Table, func() error, error) {
	thisTables := cd.thisTables
	nextTables := cd.nextTables
	options := &model.Options{IsAsc: true}

	newIterator := func() []model.Iterator {
		var iters []model.Iterator
		switch {
		case level == 0:
			iters = append(iters, iteratorsReversed(thisTables, options)...)
		case len(thisTables) > 0:
			iters = append(iters, thisTables[0].NewTableIterator(options))
		}
		return append(iters, NewConcatIterator(nextTables, options))
	}

	res := make(chan *Table, 3)

	inflightBuilders := utils.NewThrottle(8 + len(cd.splits))

	for _, kr := range cd.splits {
		if err := inflightBuilders.Do(); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)
			iterators := newIterator() // 全量表参与迭代
			iterator := NewMergeIterator(iterators, false)
			defer iterator.Close() // 逐个解开 table 引用
			lm.subCompact(iterator, kr, cd, inflightBuilders, res)
		}(kr)
	}

	var newTables []*Table
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for re := range res {
			newTables = append(newTables, re)
		}
	}()

	err := inflightBuilders.Finish()
	close(res) // 通知 done()
	wg.Wait()

	if err == nil {
		err = utils.SyncDir(lm.opt.WorkDir)
	}
	if err != nil {
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("compactBuildTables while running compactions for: %+v, %v", cd, err)
	}
	sort.Slice(newTables, func(i, j int) bool {
		return model.CompareKeyNoTs(newTables[i].sst.MaxKey(), newTables[j].sst.MaxKey()) < 0
	})
	return newTables, func() error {
		return decrRefs(newTables)
	}, nil
}

func (lm *LevelsManger) subCompact(iterator model.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *Table) {
	discardStats := make(map[uint32]int64)
	defer func() {
		go lm.updateDiscardStats(discardStats) // 重新开一个go协程,让其去阻塞,不要耽搁函数返回;
	}()
	var lastKey []byte
	var lastEntry model.Entry

	// 统计 需要通知 vlogGC的 无效key数据;
	updateDiscardStats := func(e model.Entry) {
		if e.Meta&common.BitValuePointer > 0 {
			var vp model.ValuePtr
			vp.Decode(e.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}

	// 经过判断,得到最后真正需要保留的key;
	builderAdd := func(builder *sstBuilder, e model.Entry) {
		isDeletedOrExpired := IsDeletedOrExpired(&e)
		switch {
		case isDeletedOrExpired:
			builder.AddStaleKey(&e)
		default:
			builder.AddKey(&e)
		}
	}

	// 1. 判断key是否需要保留
	// 2. 判断key是否需要通知 vlogGC
	judgeKeys := func(builder *sstBuilder) {
		var tableKr keyRange
		for ; iterator.Valid(); iterator.Next() {
			curKey := iterator.Item().Item.Key
			// 1) sst aaa:1
			// 2) sst aaa:1 aaa:5 aaa:8 bbb:6 bbb:8 ccc:5
			// 3) sst aaa:1 bbb:3 ddd:8(out)
			if lastKey == nil {
				lastKey = model.SafeCopy(lastKey, curKey)
				lastEntry = iterator.Item().Item
			}

			if len(kr.right) > 0 && model.CompareKeyNoTs(curKey, kr.right) >= 0 {
				break
			}
			if builder.ReachedCapacity() {
				break
			}
			if len(tableKr.left) == 0 {
				tableKr.left = model.SafeCopy(tableKr.left, curKey)
			}
			tableKr.right = model.SafeCopy(tableKr.right, curKey)

			// lastKey: nil     curKey: bbb:5
			// lastKey: aaa:5   curKey: bbb:5
			if !model.SameKeyNoTs(curKey, lastKey) {
				builderAdd(builder, lastEntry)
				lastKey = model.SafeCopy(lastKey, curKey)
				lastEntry = iterator.Item().Item
			} else {
				// lastKey: aaa:1  curKey: aaa:5  nextKey: aaa:9
				if model.ParseTsVersion(curKey) > model.ParseTsVersion(lastKey) {
					// skip lastKey
					updateDiscardStats(lastEntry)
					lastKey = model.SafeCopy(lastKey, curKey)
					lastEntry = iterator.Item().Item
				} else {
					// model.ParseTsVersion(curKey) <= model.ParseTsVersion(lastKey)
					// don`t do anything; continue to next key;
				}
			}
		} // for over
		// 额外情况: 假如当前 .sst 中只含有一个 entry , 那么也需要进行保存;
		if lastEntry.Key != nil {
			tableKr.right = model.SafeCopy(tableKr.right, lastEntry.Key)
			builderAdd(builder, lastEntry)
		}
	} // addKeys Over;

	if len(kr.left) > 0 {
		iterator.Seek(kr.left)
	} else {
		iterator.Rewind()
	}

	for iterator.Valid() {
		key := iterator.Item().Item.Key
		// 末尾 keyRange 最右侧 kr.right == 0
		if len(kr.right) > 0 && model.CompareKeyNoTs(key, kr.right) >= 0 {
			break
		}

		builder := newSSTBuilderWithSSTableSize(lm.opt, cd.dst.fileSize[cd.nextLevel.levelID])
		judgeKeys(builder)

		if builder.empty() {
			builder.Finish()
			continue
		}

		if err := inflightBuilders.Do(); err != nil {
			break
		}

		go func(builder *sstBuilder) {
			defer inflightBuilders.Done(nil)
			newFID := lm.NextFileID()
			ssName := utils.FileNameSSTable(lm.opt.WorkDir, newFID)
			ntl, err := OpenTable(lm, ssName, builder)
			if err != nil {
				common.Err(err)
				panic(err)
			}
			res <- ntl
		}(builder)
	} // for over
}

func IsDeletedOrExpired(e *model.Entry) bool {
	if e.Meta&common.BitDelete > 0 {
		return true
	}
	if e.Version == 0 {
		return true
	}
	if e.Meta == 0 && e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}
	return e.ExpiresAt <= uint64(time.Now().Unix())
}
func (lm *LevelsManger) updateDiscardStats(discardStats map[uint32]int64) {
	select {
	case *lm.lsm.option.DiscardStatsCh <- discardStats:
	}
}
func iteratorsReversed(tables []*Table, options *model.Options) []model.Iterator {
	out := make([]model.Iterator, 0)
	for i := len(tables) - 1; i >= 0; i-- {
		out = append(out, tables[i].NewTableIterator(options))
	}
	return out
}
func tablesToString(tables []*Table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, " . ")
	return res
}
func (lm *LevelsManger) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]
	width := len(cd.nextTables) / 2.0
	if width < 3 {
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)
	addRange := func(right []byte) {
		skr.right = model.SafeCopy(nil, right)
		cd.splits = append(cd.splits, skr)
		skr.left = skr.right
	}
	for i, t := range cd.nextTables {
		if i == len(cd.nextTables)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			right := t.sst.MaxKey()
			addRange(right)
		}
	}
}

// 5. 更新元信息
func buildChangeSet(cd *compactDef, tables []*Table) pb.ManifestChangeSet {
	var changees []*pb.ManifestChange
	for _, t := range tables {
		changees = append(changees, newCreateChange(t.fid, cd.nextLevel.levelID))
	}
	for _, t := range cd.thisTables {
		changees = append(changees, newDeleteChange(t.fid))
	}
	for _, t := range cd.nextTables {
		changees = append(changees, newDeleteChange(t.fid))
	}
	return pb.ManifestChangeSet{Changes: changees}
}
func newDeleteChange(fid uint64) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:   fid,
		Type: pb.ManifestChange_Delete,
	}
}
func newCreateChange(id uint64, level int) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:      id,
		Type:    pb.ManifestChange_Create,
		LevelId: uint32(level),
	}
}

// compactIngStatus 所有层的压缩状态信息
type compactIngStatus struct {
	mux    sync.Mutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

func (lsm *LSM) newCompactStatus() *compactIngStatus {
	cs := &compactIngStatus{
		mux:    sync.Mutex{},
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	// 0 层也需要
	for i := 0; i < lsm.option.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}
func (cs *compactIngStatus) overlapsWith(level int, key keyRange) bool {
	cs.mux.Lock()
	defer cs.mux.Unlock()
	compactStatus := cs.levels[level]
	return compactStatus.overlapsWith(key)
}
func (cs *compactIngStatus) getLevelDelSize(level int) int64 {
	return cs.levels[level].delSize
}
func (cs *compactIngStatus) deleteCompactionDef(cd compactDef) {
	cs.mux.Lock()
	defer cs.mux.Unlock()
	levelID := cd.thisLevel.levelID
	thisLevelStatus := cs.levels[cd.thisLevel.levelID]
	nextLevelStatus := cs.levels[cd.nextLevel.levelID]

	thisLevelStatus.delSize -= cd.thisSize
	found := thisLevelStatus.removeRange(cd.thisRange)
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevelStatus.removeRange(cd.nextRange) && found
	}
	if !found {
		thisR := cd.thisRange
		nextR := cd.nextRange
		fmt.Printf("Looking for: %s in this level %d.\n", thisR, levelID)
		fmt.Printf("This Level:\n%s\n", thisLevelStatus.debugPrint())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", nextR, cd.nextLevel.levelID)
		fmt.Printf("Next Level:\n%s\n", nextLevelStatus.debugPrint())
		log.Fatal("keyRange not found")
	}

	for _, t := range append(cd.thisTables, cd.nextTables...) {
		if _, ok := cs.tables[t.fid]; ok {
			delete(cs.tables, t.fid)
		} else {
			common.CondPanic(!ok, fmt.Errorf("cs.tables is nil"))
		}
	}
}
func (cs *compactIngStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.mux.Lock()
	defer cs.mux.Unlock()
	thisLevelStatus := cs.levels[cd.thisLevel.levelID]
	nextLevelStatus := cs.levels[cd.nextLevel.levelID]
	if thisLevelStatus.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevelStatus.overlapsWith(cd.nextRange) {
		return false
	}
	thisLevelStatus.ranges = append(thisLevelStatus.ranges, cd.thisRange)
	nextLevelStatus.ranges = append(nextLevelStatus.ranges, cd.nextRange)
	thisLevelStatus.delSize += cd.thisSize
	for _, t := range append(cd.thisTables, cd.nextTables...) {
		cs.tables[t.fid] = struct{}{}
	}
	return true
}

// levelCompactStatus 每一层的压缩状态信息
type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapWith(dst) {
			return true
		}
	}
	return false
}
func (lcs *levelCompactStatus) removeRange(dst keyRange) bool {
	out := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			out = append(out, r)
		} else {
			found = true
		}
	}
	lcs.ranges = out
	return found
}
func (lcs *levelCompactStatus) debugPrint() string {
	var buf bytes.Buffer
	for _, r := range lcs.ranges {
		buf.WriteString(r.String())
	}
	return buf.String()
}

// keyRange key 区间
type keyRange struct {
	left  []byte
	right []byte
	inf   bool  // 是否合并过
	size  int64 // size is used for Key splits.
}

func getKeyRange(tables ...*Table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	// 此时的key 是带有 ts 时间戳;
	minKey := tables[0].sst.MinKey()
	maxKey := tables[0].sst.MaxKey()
	for i := 1; i < len(tables); i++ {
		if model.CompareKeyNoTs(tables[i].sst.MinKey(), minKey) < 0 {
			minKey = tables[i].sst.MinKey()
		}
		if model.CompareKeyNoTs(tables[i].sst.MaxKey(), maxKey) > 0 {
			maxKey = tables[i].sst.MaxKey()
		}
	}
	return keyRange{
		left:  minKey,
		right: maxKey,
	}
}
func (key keyRange) isEmpty() bool {
	return len(key.left) == 0 && len(key.right) == 0 && !key.inf
}
func (key keyRange) String() string {
	return fmt.Sprintf("[left=%x,right=%x,inf=%v", key.left, key.right, key.inf)
}
func (key keyRange) equals(dst keyRange) bool {
	return bytes.Equal(key.left, dst.left) && bytes.Equal(key.right, dst.right) && key.inf == dst.inf
}
func (key *keyRange) extend(dst keyRange) {
	if dst.isEmpty() {
		return
	}
	if key.isEmpty() {
		*key = dst
	}
	if len(key.left) == 0 || model.CompareKeyNoTs(dst.left, key.left) < 0 {
		key.left = dst.left
	}
	if len(key.right) == 0 || model.CompareKeyNoTs(dst.right, key.right) > 0 {
		key.right = dst.right
	}
	if dst.inf {
		key.inf = true
	}
}
func (key keyRange) overlapWith(dst keyRange) bool {
	if key.isEmpty() {
		return true
	}
	if dst.isEmpty() {
		return false
	}
	if key.inf || dst.inf {
		return true
	}

	if model.CompareKeyNoTs(key.left, dst.right) > 0 {
		return false
	}
	if model.CompareKeyNoTs(key.right, dst.left) < 0 {
		return false
	}
	return true
}
