package lsm

import (
	"github.com/stretchr/testify/require"
	"testing"
	"trainKv/common"
	"trainKv/model"
	"trainKv/pb"
	"trainKv/utils"
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

func createEmptyTable(lsm *LSM) *table {
	b := newSSTBuilder(compactOptions)
	defer b.close()
	// Add one key so that we can open this table.
	b.add(model.Entry{Key: model.KeyWithTestTs([]byte("foo"), uint64(1)), Value: []byte{}}, false)
	fileName := utils.FileNameSSTable(compactOptions.WorkDir, lsm.levelManger.reserveFileID())
	tab := openTable(&levelsManger{opt: &Options{SSTableMaxSz: compactOptions.SSTableMaxSz}}, fileName, b)
	return tab
}

// createAndSetLevel creates a table with the given data and adds it to the given level.
func createAndSetLevel(lsm *LSM, td []keyValVersion, level int) {
	builder := newSSTBuilder(compactOptions)
	defer builder.close()

	// Add all keys and versions to the table.
	for _, item := range td {
		key := model.KeyWithTestTs([]byte(item.key), uint64(item.version))
		val := model.ValueExt{Value: []byte(item.val), Meta: item.meta}
		e := model.NewEntry(key, val.Value)
		builder.add(e, false)
	}
	fileName := utils.FileNameSSTable(compactOptions.WorkDir, lsm.levelManger.reserveFileID())
	tab := openTable(lsm.levelManger, fileName, builder)
	if err := lsm.levelManger.manifestFile.addChanges([]*pb.ManifestChange{newCreateChange(tab.fid, level)}); err != nil {
		panic(err)
	}
	lsm.levelManger.levelHandlers[level].mux.Lock()
	// Add table to the given level.
	lsm.levelManger.levelHandlers[level].tables = append(lsm.levelManger.levelHandlers[level].tables, tab)
	lsm.levelManger.levelHandlers[level].mux.Unlock()
}

type keyValVersion struct {
	key     string
	val     string
	version int
	meta    byte
}

// TestCheckOverlap 测试重叠表区间
func TestCheckOverlap(t *testing.T) {
	t.Run("overlap", func(t *testing.T) {
		// This test consists of one table on level 0 and one on level 1.
		// There is an overlap amongst the tables but there is no overlap with rest of the levels.
		t.Run("same keys", func(t *testing.T) {
			runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				l0 := []keyValVersion{{"foo", "bar", 3, 0}}
				l1 := []keyValVersion{{"foo", "bar", 2, 0}}
				createAndSetLevel(lsm, l0, 0) // 0层的是高版本
				createAndSetLevel(lsm, l1, 1) // 1层的是低版本

				// Level 0 should overlap with level 0 tables.
				require.True(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 0))

				// Level 1 should overlap with level 0 tables (they have the same keys).
				require.True(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 1))

				// Level 2 and 3 should not overlap with level 0 tables.
				require.False(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 2))
				require.False(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[1].tables, 2))

				require.False(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 3))
				require.False(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[1].tables, 3))

			})
		})

		t.Run("overlapping keys", func(t *testing.T) {
			runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				l0 := []keyValVersion{
					{"a", "x", 1, 0},
					{"b", "x", 1, 0},
					{"foo", "bar", 3, 0}}
				l1 := []keyValVersion{
					{"foo", "bar", 2, 0}}
				createAndSetLevel(lsm, l0, 0)
				createAndSetLevel(lsm, l1, 1)

				// Level 0 should overlap with level 0 tables.
				require.True(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 0))
				require.True(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[1].tables, 1))

				// Level 1 should overlap with level 0 tables, "foo" key is common.
				require.True(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 1))

				// Level 2 and 3 should not overlap with level 0 tables.
				require.False(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 2))
				require.False(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 3))
			})
		})
	})

	t.Run("non-overlapping", func(t *testing.T) {
		runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
			l0 := []keyValVersion{
				{"a", "x", 1, 0},
				{"b", "x", 1, 0},
				{"c", "bar", 3, 0}}
			l1 := []keyValVersion{
				{"foo", "bar", 2, 0}}
			createAndSetLevel(lsm, l0, 0)
			createAndSetLevel(lsm, l1, 1)

			// Level 1 should not overlap with level 0 tables
			require.False(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 1))

			// Level 2 and 3 should not overlap with level 0 tables.
			require.False(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 2))
			require.False(t, lsm.levelManger.checkOverlap(lsm.levelManger.levelHandlers[0].tables, 3))
		})
	})
}

func TestCompaction(t *testing.T) {
	t.Run("level 0 to level 1", func(t *testing.T) {
		runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
			l0 := []keyValVersion{
				{"foo", "bar", 3, 0},
				{"fooh", "barh", 1, 0}}
			l01 := []keyValVersion{
				{"fooa", "bara", 2, 0},
				{"foob", "barb", 3, 0},
				{"fooc", "barc", 4, 0},
			}
			l1 := []keyValVersion{
				{"foog", "barg", 5, 0},
				{"foox", "bargx", 6, 0},
			}

			// Level 0 has table l0 and l01.
			createAndSetLevel(lsm, l0, 0)
			createAndSetLevel(lsm, l01, 0)

			// Level 1 has table l1.
			createAndSetLevel(lsm, l1, 1)

			/*  起始数据状态
			{"foo", "bar", 3, 0},
			{"foo", "bar", 2, 0},
			{"foo", "bar", 1, 0},
			{"fooz", "baz", 1, 0},
			*/
			cdef := compactDef{
				thisLevel:  lsm.levelManger.levelHandlers[0],
				nextLevel:  lsm.levelManger.levelHandlers[1],
				thisTables: lsm.levelManger.levelHandlers[0].tables,
				nextTables: lsm.levelManger.levelHandlers[1].tables,
				dst:        lsm.levelManger.levelTargets(), // 计算的 baseLevel 会直接到6层;
			}
			cdef.dst.dstLevelId = 1
			require.NoError(t, lsm.levelManger.runCompactDef(-1, 0, cdef))
			// foo version 2,1 should be dropped after compaction.
			// 合并后的数据状态
			//{"foo", "bar", 3, 0},
			//{"fooz", "baz", 1, 0}})
		})
	})
}

func runBadgerTest(t *testing.T, opts *Options, test func(t *testing.T, lsm *LSM)) {
	if opts == nil {
		opts = compactOptions
	}
	c := make(chan map[uint32]int64)
	compactOptions.DiscardStatsCh = &c
	clearDir(opts.WorkDir)
	lsm := NewLSM(opts)
	defer lsm.Close()
	test(t, lsm)
}
