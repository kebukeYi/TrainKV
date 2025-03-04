package lsm

import (
	"bytes"
	"fmt"
	"testing"
	"time"
	"trainKv/common"
	"trainKv/model"
)

var compactOptions = &Options{
	WorkDir:             "/usr/projects_gen_data/goprogendata/trainkvdata/test/compact",
	SSTableMaxSz:        10240,
	MemTableSize:        10240,
	BlockSize:           400,
	BloomFalsePositive:  0.1,
	BaseLevelSize:       2 << 20, //2,097,152 B
	LevelSizeMultiplier: 10,
	BaseTableSize:       1 << 20, // 1,048,576 B
	TableSizeMultiplier: 2,
	NumLevelZeroTables:  15,
	MaxLevelNum:         common.MaxLevelNum,
	NumCompactors:       3,
}

func buildLSM() *LSM {
	lsm := NewLSM(compactOptions)
	return lsm
}

func TestCompact(t *testing.T) {
	clearDir(compactOptions.WorkDir)
	lsm := buildLSM()
	ok := false

	l0TOLMax := func() {
		// 正常触发即可
		baseTest(t, lsm, 128)
		// 直接触发压缩执行
		fid := lsm.levelManger.maxFID + 1
		lsm.levelManger.runOnce(1)
		for _, t := range lsm.levelManger.levelHandlers[6].tables {
			if t.fid == fid {
				ok = true
			}
		}
		common.CondPanic(!ok, fmt.Errorf("[l0TOLMax] fid not found"))
	}

	l0ToL0 := func() {
		// 先写一些数据进来
		baseTest(t, lsm, 128)
		fid := lsm.levelManger.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 0)
		// 手动设置一些tale属性, 以达到合并要求;
		tricky(cd.thisLevel.tables)
		ok := lsm.levelManger.findTablesL0ToL0(cd)
		common.CondPanic(!ok, fmt.Errorf("[l0ToL0] lsm.levels.fillTablesL0ToL0(cd) ret == false"))
		err := lsm.levelManger.runCompactDef(0, 0, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levelManger.compactIngStatus.deleteCompactionDef(*cd)
		common.Err(err)
		ok = false
		for _, t := range lsm.levelManger.levelHandlers[0].tables {
			if t.fid == fid {
				ok = true
			}
		}
		common.CondPanic(!ok, fmt.Errorf("[l0ToL0] fid not found"))
	}

	nextCompact := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levelManger.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 1)
		// 手动设置一些tale属性, 以达到合并要求;
		tricky(cd.thisLevel.tables)
		ok := lsm.levelManger.findTables(cd)
		common.CondPanic(!ok, fmt.Errorf("[nextCompact] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levelManger.runCompactDef(0, 0, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levelManger.compactIngStatus.deleteCompactionDef(*cd)
		common.Err(err)
		ok = false
		for _, t := range lsm.levelManger.levelHandlers[1].tables {
			if t.fid == fid {
				ok = true
			}
		}
		common.CondPanic(!ok, fmt.Errorf("[nextCompact] fid not found"))
	}

	maxToMax := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levelManger.maxFID + 1
		cd := buildCompactDef(lsm, 6, 6, 6)
		// 手动设置一些tale属性, 以达到合并要求;
		tricky(cd.thisLevel.tables)
		ok := lsm.levelManger.findTables(cd)
		common.CondPanic(!ok, fmt.Errorf("[maxToMax] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levelManger.runCompactDef(0, 6, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levelManger.compactIngStatus.deleteCompactionDef(*cd)
		common.Err(err)
		ok = false
		for _, t := range lsm.levelManger.levelHandlers[6].tables {
			if t.fid == fid {
				ok = true
			}
		}
		common.CondPanic(!ok, fmt.Errorf("[maxToMax] fid not found"))
	}

	parallerCompact := func() {
		baseTest(t, lsm, 128)
		cd := buildCompactDef(lsm, 0, 0, 1)
		// 手动设置一些tale属性, 以达到合并要求;
		tricky(cd.thisLevel.tables)
		ok := lsm.levelManger.findTables(cd)
		common.CondPanic(!ok, fmt.Errorf("[parallerCompact] lsm.levels.fillTables(cd) ret == false"))
		// 构建完全相同两个压缩计划的执行，以便于百分比构建 压缩冲突
		go lsm.levelManger.runCompactDef(0, 0, *cd)
		lsm.levelManger.runCompactDef(0, 0, *cd)
		// 检查compact status状态查看是否在执行并行压缩
		isParaller := false
		for _, state := range lsm.levelManger.compactIngStatus.levels {
			if len(state.ranges) != 0 {
				isParaller = true
			}
		}
		common.CondPanic(!isParaller, fmt.Errorf("[parallerCompact] not is paralle"))
	}

	// 运行N次测试多个sst的影响
	runTest(1, l0TOLMax, l0ToL0, nextCompact, maxToMax, parallerCompact)
}

// 正确性测试
func baseTest(t *testing.T, lsm *LSM, n int) {
	// 用来跟踪调试的
	e := &model.Entry{
		Key:       []byte("CRTS硬核课堂MrGSBtL12345678"),
		Value:     []byte("跟踪调试"),
		ExpiresAt: 123,
	}
	//caseList := make([]*utils.Entry, 0)
	//caseList = append(caseList, e)

	// 随机构建数据进行测试
	lsm.Put(e)
	for i := 1; i < n; i++ {
		ee := model.BuildEntry()
		lsm.Put(ee)
		// caseList = append(caseList, ee)
	}
	// 从levels中进行GET
	v, err := lsm.Get(e.Key)
	common.Panic(err)
	common.CondPanic(!bytes.Equal(e.Value, v.Value), fmt.Errorf("lsm.Get(e.Key) value not equal !!!"))

	//// TODO range功能待完善
	//retList := make([]*utils.Entry, 0)
	//testRange := func(isAsc bool) {
	//	// Range 确保写入进去的每个lsm都可以被读取到
	//	iter := lsm.NewIterators(&utils.Options{IsAsc: true})
	//	for iter.Rewind(); iter.Valid(); iter.Next() {
	//		e := iter.Item().Entry()
	//		retList = append(retList, e)
	//	}
	//	utils.CondPanic(len(retList) != len(caseList), fmt.Errorf("len(retList) != len(caseList)"))
	//	sort.Slice(retList, func(i, j int) bool {
	//		return utils.CompareKeys(retList[i].Key, retList[j].Key) > 1
	//	})
	//	for i := 0; i < len(caseList); i++ {
	//		a, b := caseList[i], retList[i]
	//		if !equal(a.Key, b.Key) || !equal(a.Value, b.Value) || a.ExpiresAt != b.ExpiresAt {
	//			utils.Panic(fmt.Errorf("lsm.Get(e.Key) kv disagreement !!!"))
	//		}
	//	}
	//}
	//// 测试升序
	//testRange(true)
	//// 测试降序
	//testRange(false)
}

// 运行测试用例
func runTest(n int, testFunList ...func()) {
	for _, f := range testFunList {
		for i := 0; i < n; i++ {
			f()
		}
	}
}

// 构建compactDef对象
func buildCompactDef(lsm *LSM, id, thisLevel, nextLevel int) *compactDef {
	t := targets{
		levelTargetSSize: []int64{0, 10485760, 10485760, 10485760, 10485760, 10485760, 10485760},
		fileSize:         []int64{1024, 2097152, 2097152, 2097152, 2097152, 2097152, 2097152},
		dstLevelId:       nextLevel,
	}
	def := &compactDef{
		compactorId: id,
		thisLevel:   lsm.levelManger.levelHandlers[thisLevel],
		nextLevel:   lsm.levelManger.levelHandlers[nextLevel],
		dst:         t,
		prior:       buildCompactionPriority(lsm, thisLevel, t),
	}
	return def
}

// 构建CompactionPriority对象
func buildCompactionPriority(lsm *LSM, thisLevel int, t targets) compactionPriority {
	return compactionPriority{
		levelId:  thisLevel,
		score:    8.6,
		adjusted: 860,
		dst:      t,
	}
}

// 手动设置一些tale属性, 以达到合并要求;
func tricky(tables []*table) {
	for _, table := range tables {
		table.sst.Indexs().StaleDataSize = 10 << 20
		t, _ := time.Parse("2006-01-02 15:04:05", "1995-08-10 00:00:00")
		table.sst.SetCreatedAt(&t)
	}
}
