package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/interfaces"
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
	levelTargetSSize []int64 // 对应 层中所有 .sst 文件的期望总大小; 用于计算 每层的优先级;
	fileSize         []int64 // 对应 层中单个 .sst 文件的期望大小; 用于设定 合并的生成的目标 sst 文件大小;
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

func (cd *compactDef) allTables() []*Table {
	tables := make([]*Table, 0, len(cd.thisTables)+len(cd.nextTables))
	tables = append(tables, cd.thisTables...)
	tables = append(tables, cd.nextTables...)
	return tables
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
	// 计算各个level的待合并分数; 按照大小排列,
	prios := lm.pickCompactLevels()
	// 如果是0号协程, 那么就优先合并 l0 层;
	if compactorId == 0 {
		// 将 合并level-0层的优先级调高;
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
	// compactorId, for:p 是将要参与合并的 X层源头层级, 此时目标 Y层 已经找好了;
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
		// 反推上面各层“理论上”该有多大;
		dst.levelTargetSSize[i] = levelTargetSize
		// 从下往上,谁先缩到 BaseLevelSize 以下, 谁就站出来当 base level
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
	// 既然本层空、下一层又吃得下，那就干脆再往下跳一层，让数据一次沉到位，省得以后再搬;
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

	// 如果 Li-1 的score > 1.0 ; 那么 Li-1 = Li-1/Li; 否则下探;
	// 假如 Li.score >= 1.0, Li-1.score 就会变小, 那么我们就倾向于 Li层的压缩;
	// 假如 Li.score < 1.0,  Li-1.score 就会变大, 那么我们就倾向于 Li-1层的压缩;
	var preLevel int
	for level := levelTargets.dstLevelId; level < len(lm.levelHandlers); level++ {
		if prios[preLevel].adjusted > 1.0 {
			// 如果当前层分数太小（<0.01），就改用 0.01 当分母，防止除出一个爆炸大数
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

	// 当前 level0层 需要合并;
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
	log.Printf("[Compactor: %d] Compaction for level: %d to %d DONE",
		id, cd.thisLevel.levelID, cd.nextLevel.levelID)

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

	var xInTables []*Table
	var kr keyRange
	for _, t := range xTables {
		tkr := getKeyRange(t)
		if kr.overlapWith(tkr) {
			xInTables = append(xInTables, t)
			kr.extend(tkr)
		} else {
			break
		}
	}
	cd.thisRange = getKeyRange(xInTables...)
	cd.thisTables = xInTables

	left, right := cd.nextLevel.findOverLappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.nextTables = make([]*Table, right-left)
	copy(cd.nextTables, cd.nextLevel.tables[left:right])

	if len(cd.nextTables) == 0 {
		// 当下层（bot，通常是下一层）没有需要合并的文件时,
		// 此设置目的是让下一次压缩的范围不允许为当前压缩的范围, 当前继续压缩;
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
	var xInTables []*Table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.dst.fileSize[0] {
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			continue
		}
		if _, has := lm.compactIngStatus.tables[t.fid]; has {
			continue
		}
		xInTables = append(xInTables, t)
	}
	if len(xInTables) < 4 {
		return false
	}
	// 强制重叠;
	cd.thisRange = keyRange{inf: true}
	cd.thisTables = xInTables

	compactStatus := lm.compactIngStatus.levels[cd.thisLevel.levelID]
	compactStatus.ranges = append(compactStatus.ranges, cd.thisRange)

	for _, t := range xInTables {
		lm.compactIngStatus.tables[t.fid] = struct{}{}
	}
	// LO->L0: 把一堆重叠的小文件合并成一个大文件, 减少文件数量;
	cd.dst.fileSize[0] = math.MaxUint32
	return true
}

// 4.3. lmax -> lmax
// 4.4. lx -> lx+1
func (lm *LevelsManger) findTables(cd *compactDef) bool {
	cd.lockLevel()
	defer cd.unlockLevel()

	tables := make([]*Table, len(cd.thisLevel.tables))
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}

	if cd.thisLevel.isLastLevel() {
		return lm.findMaxLevelTables(tables, cd)
	}
	// 所有文件先按照 版本号升序排序;(默认认为版本号越低,数据越老,越该合并清理掉)
	lm.sortByMaxVersion(tables, cd)
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

		// 当前 t 在下层没有 相关的 .sst 文件;
		if len(cd.nextTables) == 0 {
			cd.nextTables = []*Table{}
			cd.nextRange = cd.thisRange
			// 设置当前层和下层的合并区间信息;
			if !lm.compactIngStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				// cas 失败代表: 说明两层区间,在其他协程中参与合并了, 跳过当前t;
				continue
			}
			// cas 成功代表: 说明当前表没有参与合并, 但是为什么直接返回了呢?
			// 答: 当下一层没有重叠的表时, 说明这个表也可以被移动到下一层中;
			return true
		}

		cd.nextRange = getKeyRange(cd.nextTables...)

		// 再次检测 是否存在重叠的合并表;
		if lm.compactIngStatus.overlapsWith(cd.nextLevel.levelID, cd.nextRange) {
			continue
		}

		// 不存在重叠的合并表, 那就执行cas;
		if !lm.compactIngStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		// cas 成功代表: 说明当前表没有参与合并,返回;
		return true
	}
	return false
}

func (lm *LevelsManger) sortByMaxVersion(table []*Table, cd *compactDef) {
	if len(table) == 0 || cd.nextLevel == nil {
		return
	}
	sort.Slice(table, func(i, j int) bool {
		return table[i].MaxVersion() < table[j].MaxVersion()
	})
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
		nextIdx := sort.Search(len(tables), func(i int) bool {
			return model.CompareKeyWithTs(tables[i].sst.minKey, t.sst.minKey) >= 0
		})
		common.CondPanic(tables[nextIdx].fid != t.fid, errors.New("tables[j].ID() != t.ID()"))
		totalSize := t.Size()
		nextIdx++
		for nextIdx < len(tables) {
			totalSize += tables[nextIdx].Size()
			if totalSize >= needSz {
				break
			}
			cd.nextTables = append(cd.nextTables, tables[nextIdx])
			cd.nextRange.extend(getKeyRange(tables[nextIdx]))
			nextIdx++
		}
	}

	now := time.Now()
	for _, t := range sortedTables {
		if t.MaxVersion() > lm.getDiscardTs() {
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < time.Hour {
			continue
		}
		// 小于 10MB 不合并;
		if t.getStaleDataSize() < common.LevelMaxStaleDataSize {
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
		// tableSize 已经足够大了, 找到就返回;
		if t.Size() >= needFileSize {
			break
		}
		// 文件不够大, 继续搜寻;
		collectNextTables(t, needFileSize)
		if !lm.compactIngStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.nextTables = cd.nextTables[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	} // for over

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
	common.CondPanic(len(cd.splits) != 0, errors.New("#runCompactDef, len(cd.splits) != 0"))
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
	options := &interfaces.Options{IsAsc: true, IsSetCache: false}

	newIterator := func() []interfaces.Iterator {
		var iters []interfaces.Iterator
		switch {
		case level == 0:
			iters = append(iters, iteratorsReversed(thisTables, options)...)
		case len(thisTables) > 0:
			utils.AssertTrue(len(thisTables) == 1)
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
			iterators := newIterator() // 全量表参与迭代;
			iterator := NewMergingIterator(iterators, options)
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
		return model.CompareKeyWithTs(newTables[i].sst.MaxKey(), newTables[j].sst.MaxKey()) < 0
	})
	return newTables, func() error {
		return decrRefs(newTables)
	}, nil
}

func (lm *LevelsManger) subCompact(iterator interfaces.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *Table) {
	discardStats := make(map[uint32]int64)
	defer func() {
		go lm.updateDiscardStats(discardStats) // 重新开一个go协程,让其去阻塞,不要耽搁函数返回;
	}()

	// 存在的话, 不能轻易删除数据, 比如高版本的 delete 标记, 随便删除的话, 后续可能读到下层的旧数据;
	hasOverlap := lm.checkOverlap(cd.allTables(), cd.nextLevel.levelID+1)

	// 判断是否可提前 进行sst 构造; 这有助于避免在Li上生成与Li+1有大量重叠的表;
	exceedsAllowedOverlap := func(kr keyRange) bool {
		nextNextLevel := cd.nextLevel.levelID + 1
		if nextNextLevel <= 1 || nextNextLevel >= lm.opt.MaxLevelNum {
			return false
		}
		nnl := lm.levelHandlers[nextNextLevel]
		nnl.mux.RLock()
		defer nnl.mux.RUnlock()
		// 判断 和将要参与合并的下下一层 table 数量重叠次数是否 过多;
		// 避免重叠区间太多, 引发写放大;
		left, right := nnl.findOverLappingTables(levelHandlerRLocked{}, kr)
		return right-left >= 10
	}

	// 统计 需要通知 vlogGC 的 无效key数据;
	updateDiscardStats := func(e model.Entry) {
		if e.Meta&common.BitValuePointer > 0 {
			var vp model.ValuePtr
			vp.Decode(e.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}

	var (
		lastKey, skipKey []byte
		keyNumVersions   int // 相同key,不同版本的个数;
	)

	// 1. 判断key是否需要保留;
	// 2. 判断key是否需要通知 vlogGC;
	addKeys := func(builder *sstBuilder) {
		timeStart := time.Now()
		var tableRange keyRange
		var numKeys, numSkips uint64
		var rangeCheck int
		for ; iterator.Valid(); iterator.Next() {
			entry := iterator.Item().Item
			if skipKey != nil || len(skipKey) > 0 {
				if model.SameKeyNoTs(entry.Key, skipKey) {
					numSkips++
					updateDiscardStats(entry)
					continue
				}
			} else {
				skipKey = skipKey[:0]
			}

			if !model.SameKeyNoTs(entry.Key, lastKey) {
				if len(kr.right) > 0 && bytes.Compare(entry.Key, kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					break
				}
				lastKey = model.SafeCopy(lastKey, entry.Key)
				if len(tableRange.left) == 0 {
					tableRange.left = model.SafeCopy(tableRange.left, entry.Key)
				}
				tableRange.right = lastKey
				keyNumVersions = 0
				rangeCheck++
				if rangeCheck%lm.opt.SSTKeyRangCheckNums == 0 {
					if exceedsAllowedOverlap(tableRange) {
						break
					}
				}
			} // different key over;

			version := model.ParseTsVersion(entry.Key)
			expired := IsDeletedOrExpired(&entry)

			if version <= lm.getDiscardTs() {
				// 跟踪此键遇到的低版本数量, 只考虑 startMark 以下的版本;
				// 否则, 我们可能会丢弃正在运行的事务的唯一有效版本;
				keyNumVersions++
				lastKeyVersion := keyNumVersions == lm.opt.NumVersionsToKeep
				// 进行判断是否需要保留:
				if expired || lastKeyVersion {
					// 只有在遇到 删除标记 或者 key的保留数量达到阈值时, 才会跳过后面的不同版本key(原生key相同);
					skipKey = model.SafeCopy(skipKey, entry.Key)
					// 设置后续的key需要跳过后, 再进行判断 当前key 是否需要保留:
					switch {
					case !expired && lastKeyVersion:
					// 情况1: 不是删除标记 && 但是key版本数量够了; 当前key不用删除, 把后续的版本key清除即可;
					// 保留;
					case hasOverlap:
						// 情况2: (是删除标记 || (不是删除标记 && key版本保留可够也可不够) && 和nnL层有重叠区间;
						// 保留;
					default:
						// 其余情况: 删除标记 || 重叠区间不多 || key保留版本数量足够;
						numSkips++
						updateDiscardStats(entry)
						continue
					}
				}
			} // version <= startMarkTs

			// version > startMarkTs; 不跳过 任何版本有效的 key;
			numKeys++
			switch {
			case expired:
				builder.AddStaleKey(&entry)
			default:
				builder.AddKey(&entry)
			}
		} // for over
		fmt.Printf("[%d] LOG Compact. Added %d keys. Skipped %d keys. Iteration took: %v", cd.compactorId, numKeys, numSkips, time.Since(timeStart).Round(time.Millisecond))
	} // addKeys Over

	if len(kr.left) > 0 {
		iterator.Seek(kr.left)
	} else {
		iterator.Rewind()
	}

	for iterator.Valid() {
		key := iterator.Item().Item.Key
		// 如果存在 右区间 && 当前 key > 右区间, 说明遍历到头, 跳出循环;
		if len(kr.right) > 0 && model.CompareKeyWithTs(key, kr.right) >= 0 {
			break
		}
		builder := newSSTBuilderWithSSTableSize(lm.opt, cd.dst.fileSize[cd.nextLevel.levelID])
		addKeys(builder)

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
func iteratorsReversed(tables []*Table, options *interfaces.Options) []interfaces.Iterator {
	out := make([]interfaces.Iterator, 0)
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
	// 向上去整
	width := int(math.Ceil(float64(len(cd.nextTables)) / 5.0))
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
		// 每个区间的右边界, 包含右界的所有记录(无论版本号是多少);
		if i%width == width-1 {
			right := model.KeyWithTs(model.ParseKey(t.sst.MaxKey()), 0)
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
	mux    sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

func (lsm *LSM) newCompactStatus() *compactIngStatus {
	cs := &compactIngStatus{
		mux:    sync.RWMutex{},
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
	cs.mux.RLock()
	defer cs.mux.RUnlock()
	compactStatus := cs.levels[level]
	return compactStatus.overlapsWith(key)
}
func (cs *compactIngStatus) getLevelDelSize(level int) int64 {
	cs.mux.RLock()
	defer cs.mux.RUnlock()
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

	// 跨层压缩;
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevelStatus.removeRange(cd.nextRange) && found
	}

	if !found {
		thisR := cd.thisRange
		nextR := cd.nextRange
		fmt.Printf("Looking for XRange: %s in this level %d.\n", thisR, levelID)
		fmt.Printf("This LevelStatus:\n%s\n", thisLevelStatus.debugPrint())
		fmt.Println()
		fmt.Printf("Looking for YRange: %s in next level %d.\n", nextR, cd.nextLevel.levelID)
		fmt.Printf("Next LevelStatus:\n%s\n", nextLevelStatus.debugPrint())
		log.Fatal("keyRange not found")
	}

	for _, t := range append(cd.thisTables, cd.nextTables...) {
		if _, ok := cs.tables[t.fid]; ok {
			delete(cs.tables, t.fid)
		} else {
			common.CondPanic(!ok, fmt.Errorf("#deleteCompactionDef cs.tables is nil"))
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
	// cd.nextRange 有可能为 cd.thisRange;
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
	inf   bool  // 强制认为一定重叠;
	size  int64 // size is used for Key splits.
}

func getKeyRange(tables ...*Table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	// 此时的 key 是带有 ts 版本;
	minKey := tables[0].sst.MinKey()
	maxKey := tables[0].sst.MaxKey()
	for i := 1; i < len(tables); i++ {
		if model.CompareKeyWithTs(tables[i].sst.MinKey(), minKey) < 0 {
			minKey = tables[i].sst.MinKey()
		}
		if model.CompareKeyWithTs(tables[i].sst.MaxKey(), maxKey) > 0 {
			maxKey = tables[i].sst.MaxKey()
		}
	}
	// 这个‘超区间’，就能把最小到最大 key 之间的所有版本（无论新旧）一次性圈进来，不会漏掉任何一条记录;
	minKey = model.KeyWithTs(model.ParseKey(minKey), math.MaxUint64)
	maxKey = model.KeyWithTs(model.ParseKey(maxKey), 0)
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
	if len(key.left) == 0 || model.CompareKeyWithTs(dst.left, key.left) < 0 {
		key.left = dst.left
	}
	if len(key.right) == 0 || model.CompareKeyWithTs(dst.right, key.right) > 0 {
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

	if model.CompareKeyWithTs(key.left, dst.right) > 0 {
		return false
	}
	if model.CompareKeyWithTs(key.right, dst.left) < 0 {
		return false
	}
	return true
}
