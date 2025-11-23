package lsm

import (
	"errors"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/kebukeYi/TrainKV/pb"
	"github.com/kebukeYi/TrainKV/utils"
)

var compactTestPath = "/usr/golanddata/trainkv/compact"

var compactOptions = &Options{
	WorkDir:             compactTestPath,
	MemTableSize:        10 << 10, // 10KB; 64 << 20(64MB)
	NumFlushMemtables:   1,        // 默认：15;
	BlockSize:           2 * 1024, // 4 * 1024
	BloomFalsePositive:  0.01,     // 误差率
	CacheNums:           1 * 1024, // 10240个
	ValueThreshold:      1,        // 1B; 1 << 20(1MB)
	ValueLogMaxEntries:  100,      // 1000000
	ValueLogFileSize:    1 << 29,  // 512MB; 1<<30-1(1GB);
	VerifyValueChecksum: false,    // false

	MaxBatchCount: 100,
	MaxBatchSize:  10 << 20, // 10 << 20(10MB)

	NumCompactors:       2,       // 4
	BaseLevelSize:       8 << 20, //8MB; 10 << 20(10MB)
	LevelSizeMultiplier: 10,
	TableSizeMultiplier: 2,
	BaseTableSize:       2 << 20, // 2 << 20(2MB)
	NumLevelZeroTables:  5,
	MaxLevelNum:         common.MaxLevelNum,
}

func createEmptyTable(lsm *LSM) *Table {
	b := NewSSTBuilder(compactOptions)
	defer b.close()
	// Add one key so that we can open this table.
	entry := model.Entry{Key: model.KeyWithTestTs([]byte("foo"), uint64(1)), Value: []byte{}}
	b.Add(&entry, false)
	fileName := utils.FileNameSSTable(compactOptions.WorkDir, lsm.LevelManger.NextFileID())
	tab, _ := OpenTable(&LevelsManger{opt: &Options{BaseTableSize: compactOptions.BaseTableSize}}, fileName, b)
	return tab
}

// createAndSetLevel creates a Table with the given data and adds it to the given level.
func createAndSetLevel(lsm *LSM, td []keyValVersion, level int) {
	builder := NewSSTBuilder(compactOptions)
	defer builder.close()

	if len(td[0].key) == 1 {
		alphabet, err := generateAlphabet(td[0].key, td[len(td)-1].key)
		if err != nil {
			panic(err)
		}
		for _, c := range alphabet {
			key := model.KeyWithTestTs([]byte(c), uint64(td[0].version))
			// val := model.ValueExt{Value: []byte(item.val), Meta: item.meta}
			e := model.NewEntry(key, []byte(td[0].val))
			e.Meta = td[0].meta
			builder.Add(e, false)
		}

	} else {
		// Add all keys and versions to the table.
		for _, item := range td {
			key := model.KeyWithTestTs([]byte(item.key), uint64(item.version))
			// val := model.ValueExt{Value: []byte(item.val), Meta: item.meta}
			e := model.NewEntry(key, []byte(item.val))
			e.Meta = item.meta
			builder.Add(e, false)
		}
	}

	fileName := utils.FileNameSSTable(compactOptions.WorkDir, lsm.LevelManger.NextFileID())
	tab, _ := OpenTable(lsm.LevelManger, fileName, builder)
	if err := lsm.LevelManger.manifestFile.addChanges([]*pb.ManifestChange{newCreateChange(tab.fid, level)}); err != nil {
		panic(err)
	}
	lsm.LevelManger.levelHandlers[level].mux.Lock()
	// Add table to the given level.
	lsm.LevelManger.levelHandlers[level].tables = append(lsm.LevelManger.levelHandlers[level].tables, tab)
	lsm.LevelManger.levelHandlers[level].addSize(tab)
	lsm.LevelManger.levelHandlers[level].mux.Unlock()
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
				require.True(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 0))

				// Level 1 should overlap with level 0 tables (they have the same keys).
				require.True(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 1))

				// Level 2 and 3 should not overlap with level 0 tables.
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 2))
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[1].tables, 2))

				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 3))
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[1].tables, 3))

			})
		})

		t.Run("overlapping keys", func(t *testing.T) {
			runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				l0 := []keyValVersion{
					{"aa", "x", 1, 0},
					{"bb", "x", 1, 0},
					{"foo", "bar", 3, 0}}
				l1 := []keyValVersion{
					{"foo", "bar", 2, 0}}

				createAndSetLevel(lsm, l0, 0)
				createAndSetLevel(lsm, l1, 1)

				// Level 0 should overlap with level 0 tables.
				require.True(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 0))
				require.True(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[1].tables, 1))

				// Level 1 should overlap with level 0 tables, "foo" key is common.
				require.True(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 1))

				// Level 2 and 3 should not overlap with level 0 tables.
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 2))
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 3))
			})
		})
	})

	t.Run("non-overlapping", func(t *testing.T) {
		runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
			l0 := []keyValVersion{
				{"aa", "x", 1, 0},
				{"bb", "x", 1, 0},
				{"cc", "bar", 3, 0}}
			l1 := []keyValVersion{
				{"foo", "bar", 2, 0}}
			createAndSetLevel(lsm, l0, 0)
			createAndSetLevel(lsm, l1, 1)

			// Level 1 should not overlap with level 0 tables
			require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 1))

			// Level 2 and 3 should not overlap with level 0 tables.
			require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 2))
			require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 3))
		})
	})
}

func TestCompaction(t *testing.T) {
	// 简单测试 l0中的数据合并到l1, 只保留相同key的最高版本单个数据;
	t.Run("level 0 to level 1", func(t *testing.T) {
		runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
			l0 := []keyValVersion{
				{"foo", "bar", 3, 0},
				{"fooz", "baz", 1, 0}}
			l01 := []keyValVersion{
				{"foo", "bar", 2, 0}}
			l1 := []keyValVersion{
				{"foo", "bar", 1, 0}}

			// Level 0 has table l0 and l01.
			createAndSetLevel(lsm, l0, 0)  // 110B
			createAndSetLevel(lsm, l01, 0) // 88B

			// Level 1 has table l1.
			createAndSetLevel(lsm, l1, 1) // 88B
			//  起始数据状态
			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 1, 0},
				{"foo", "bar", 2, 0},
				{"foo", "bar", 3, 0},
				{"fooz", "baz", 1, 0},
			})

			cdef := compactDef{
				thisLevel:  lsm.LevelManger.levelHandlers[0],
				nextLevel:  lsm.LevelManger.levelHandlers[1],
				thisTables: lsm.LevelManger.levelHandlers[0].tables,
				nextTables: lsm.LevelManger.levelHandlers[1].tables,
				dst:        lsm.LevelManger.levelTargets(), // 计算的 baseLevel 会直接到6层;
			}
			cdef.dst.dstLevelId = 1
			require.NoError(t, lsm.LevelManger.runCompactDef(-1, 0, cdef))
			// foo version 2,1 should be dropped after compaction.
			// 合并后的库中数据状态:
			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"fooz", "baz", 1, 0}})
		})
	})

	// 简单测试 l0中的数据合并到l1, 只保留相同key的最高版本单个数据;
	t.Run("level 0 to level 1 with duplicates", func(t *testing.T) {
		runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
			l0 := []keyValVersion{
				{"fooz", "baz", 1, 0}}
			l01 := []keyValVersion{
				{"foo", "bar", 4, 0}}
			l1 := []keyValVersion{
				{"foo", "bar", 3, 0}}

			// Level 0 has table l0 and l01.
			createAndSetLevel(lsm, l0, 0)
			createAndSetLevel(lsm, l01, 0)
			// Level 1 has table l1.
			createAndSetLevel(lsm, l1, 1)

			// lsm层面的迭代器会返回数据库中所有数据,按照版本号递增,包括低版本数据;
			// db层面的迭代器会返回数据库中所有最高版本数据,不包括低版本无效数据;
			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"foo", "bar", 4, 0},
				{"fooz", "baz", 1, 0},
			})
			cdef := compactDef{
				thisLevel:  lsm.LevelManger.levelHandlers[0],
				nextLevel:  lsm.LevelManger.levelHandlers[1],
				thisTables: lsm.LevelManger.levelHandlers[0].tables,
				nextTables: lsm.LevelManger.levelHandlers[1].tables,
				dst:        lsm.LevelManger.levelTargets(),
			}
			cdef.dst.dstLevelId = 1
			require.NoError(t, lsm.LevelManger.runCompactDef(-1, 0, cdef))
			// foo version 3 (both) should be dropped after compaction.
			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 4, 0},
				{"fooz", "baz", 1, 0}})
		})
	})

	// 简单测试 l0中的数据合并到l1, 只保留相同key的最高版本单个数据;
	t.Run("level 0 to level 1 with lower overlap", func(t *testing.T) {
		runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
			l0 := []keyValVersion{
				{"foo", "bar", 4, 0},
				{"fooz", "baz", 1, 0}}
			l01 := []keyValVersion{
				{"foo", "bar", 3, 0}}
			l1 := []keyValVersion{
				{"foo", "bar", 2, 0}}
			l2 := []keyValVersion{
				{"foo", "bar", 1, 0}}
			// Level 0 has table l0 and l01.
			createAndSetLevel(lsm, l0, 0)
			createAndSetLevel(lsm, l01, 0)
			// Level 1 has table l1.
			createAndSetLevel(lsm, l1, 1)
			// Level 2 has table l2.
			createAndSetLevel(lsm, l2, 2)

			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 1, 0},
				{"foo", "bar", 2, 0},
				{"foo", "bar", 3, 0},
				{"foo", "bar", 4, 0},
				{"fooz", "baz", 1, 0},
			})
			cdef := compactDef{
				thisLevel:  lsm.LevelManger.levelHandlers[0],
				nextLevel:  lsm.LevelManger.levelHandlers[1],
				thisTables: lsm.LevelManger.levelHandlers[0].tables,
				nextTables: lsm.LevelManger.levelHandlers[1].tables,
				dst:        lsm.LevelManger.levelTargets(),
			}
			cdef.dst.dstLevelId = 1
			require.NoError(t, lsm.LevelManger.runCompactDef(-1, 0, cdef))
			// foo version 2 and version 1 should be dropped after compaction.
			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 1, 0}, // 在level2层,没有参与 l0->l1 的合并;
				{"foo", "bar", 4, 0},
				{"fooz", "baz", 1, 0},
			})
		})
	})

	// 简单测试 l1中的数据合并到l2, 只保留相同key的最高版本单个数据;
	t.Run("level 1 to level 2", func(t *testing.T) {
		runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
			l1 := []keyValVersion{
				{"foo", "bar", 3, 0},
				{"fooz", "baz", 1, 0}}
			l2 := []keyValVersion{
				{"foo", "bar", 2, 0}}

			createAndSetLevel(lsm, l1, 1)
			createAndSetLevel(lsm, l2, 2)

			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 2, 0},
				{"foo", "bar", 3, 0},
				{"fooz", "baz", 1, 0},
			})
			cdef := compactDef{
				thisLevel:  lsm.LevelManger.levelHandlers[1],
				nextLevel:  lsm.LevelManger.levelHandlers[2],
				thisTables: lsm.LevelManger.levelHandlers[1].tables,
				nextTables: lsm.LevelManger.levelHandlers[2].tables,
				dst:        lsm.LevelManger.levelTargets(),
			}
			cdef.dst.dstLevelId = 2
			require.NoError(t, lsm.LevelManger.runCompactDef(-1, 1, cdef))
			// foo version 2 should be dropped after compaction.
			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"fooz", "baz", 1, 0}}) // 理解
		})
	})

	t.Run("level 1 to level 2 with delete", func(t *testing.T) {

		// 简单测试 l0中的数据合并到l1, 只保留相同key的最高版本单个数据,尽管是删除类型数据;
		t.Run("with overlap", func(t *testing.T) {
			runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				l1 := []keyValVersion{
					{"foo", "bar", 3, common.BitDelete},
					{"fooz", "baz", 1, common.BitDelete}}
				l2 := []keyValVersion{
					{"foo", "bar", 2, 0},
				}
				l3 := []keyValVersion{
					{"foo", "bar", 1, 0},
				}
				createAndSetLevel(lsm, l1, 1)
				createAndSetLevel(lsm, l2, 2)
				createAndSetLevel(lsm, l3, 3)

				getAllAndCheck(t, lsm, []keyValVersion{
					{"foo", "bar", 1, 0},
					{"foo", "bar", 2, 0},
					{"foo", "bar", 3, common.BitDelete},
					{"fooz", "baz", 1, common.BitDelete},
				})
				// l1 -> l2
				cdef := compactDef{
					thisLevel:  lsm.LevelManger.levelHandlers[1],
					nextLevel:  lsm.LevelManger.levelHandlers[2],
					thisTables: lsm.LevelManger.levelHandlers[1].tables,
					nextTables: lsm.LevelManger.levelHandlers[2].tables,
					dst:        lsm.LevelManger.levelTargets(),
				}
				cdef.dst.dstLevelId = 2
				require.NoError(t, lsm.LevelManger.runCompactDef(-1, 1, cdef))
				// foo bar version 2 should be dropped after compaction.
				// fooz baz version 1 will remain because overlap exists, which is
				// expected because `hasOverlap` is only checked once at the
				// beginning of `compactBuildTables` method.
				// 处在l1,l2 层中的 fooz 并没有和下层l3 有overlap, 按照常规下, 是会被清理掉的, 但是为什么没有清理掉?
				// 但是处在 l1,l2 层中的 foo,和l3层有重合, 因此 hasOverlap ,也就被置为 true; 因此属于是连带效应,没有被铲除;
				// everything from level 1 is now in level 2.
				getAllAndCheck(t, lsm, []keyValVersion{
					{"foo", "bar", 1, 0},                 // 在l3层
					{"foo", "bar", 3, common.BitDelete},  // 在l2层
					{"fooz", "baz", 1, common.BitDelete}, // 在l2层
				})

				// l2 -> l3
				cdef = compactDef{
					thisLevel:  lsm.LevelManger.levelHandlers[2],
					nextLevel:  lsm.LevelManger.levelHandlers[3],
					thisTables: lsm.LevelManger.levelHandlers[2].tables,
					nextTables: lsm.LevelManger.levelHandlers[3].tables,
					dst:        lsm.LevelManger.levelTargets()}
				cdef.dst.dstLevelId = 3
				require.NoError(t, lsm.LevelManger.runCompactDef(-1, 2, cdef))
				// everything should be removed now
				getAllAndCheck(t, lsm, []keyValVersion{
					{"foo", "bar", 3, common.BitDelete},  // 在l3层
					{"fooz", "baz", 1, common.BitDelete}, // 在l2层
				}) // 理解
			})
		})

		t.Run("with bottom overlap", func(t *testing.T) {
			runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				l1 := []keyValVersion{
					{"foo", "bar", 3, common.BitDelete}}
				l2 := []keyValVersion{
					{"foo", "bar", 2, 0},
					{"fooz", "baz", 2, common.BitDelete}}
				l3 := []keyValVersion{
					{"fooz", "baz", 1, 0}}

				createAndSetLevel(lsm, l1, 1)
				createAndSetLevel(lsm, l2, 2)
				createAndSetLevel(lsm, l3, 3)

				getAllAndCheck(t, lsm, []keyValVersion{
					{"foo", "bar", 2, 0},
					{"foo", "bar", 3, common.BitDelete},
					{"fooz", "baz", 1, 0},
					{"fooz", "baz", 2, common.BitDelete},
				})
				cdef := compactDef{
					thisLevel:  lsm.LevelManger.levelHandlers[1],
					nextLevel:  lsm.LevelManger.levelHandlers[2],
					thisTables: lsm.LevelManger.levelHandlers[1].tables,
					nextTables: lsm.LevelManger.levelHandlers[2].tables,
					dst:        lsm.LevelManger.levelTargets(),
				}
				cdef.dst.dstLevelId = 2
				require.NoError(t, lsm.LevelManger.runCompactDef(-1, 1, cdef))
				// the top table at L1 doesn't overlap L3, but the bottom table at L2
				// does, delete keys should not be removed. 理解
				getAllAndCheck(t, lsm, []keyValVersion{
					{"foo", "bar", 3, common.BitDelete},
					{"fooz", "baz", 1, 0}, // 理解
					{"fooz", "baz", 2, common.BitDelete},
				})
			})
		})

		t.Run("without overlap", func(t *testing.T) {
			runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				l1 := []keyValVersion{
					{"foo", "bar", 3, common.BitDelete},
					{"fooz", "baz", 1, common.BitDelete}}
				l2 := []keyValVersion{
					{"fooo", "barr", 2, 0}}

				createAndSetLevel(lsm, l1, 1)
				createAndSetLevel(lsm, l2, 2)

				getAllAndCheck(t, lsm, []keyValVersion{
					{"foo", "bar", 3, common.BitDelete},
					{"fooo", "barr", 2, 0},
					{"fooz", "baz", 1, common.BitDelete},
				})
				cdef := compactDef{
					thisLevel:  lsm.LevelManger.levelHandlers[1],
					nextLevel:  lsm.LevelManger.levelHandlers[2],
					thisTables: lsm.LevelManger.levelHandlers[1].tables,
					nextTables: lsm.LevelManger.levelHandlers[2].tables,
					dst:        lsm.LevelManger.levelTargets(),
				}
				cdef.dst.dstLevelId = 2
				require.NoError(t, lsm.LevelManger.runCompactDef(-1, 1, cdef))
				// foo version 2 should be dropped after compaction.
				// 没有出现 重合, 删除标记,还存在;
				getAllAndCheck(t, lsm, []keyValVersion{
					{"foo", "bar", 3, common.BitDelete},
					{"fooo", "barr", 2, 0},
					{"fooz", "baz", 1, common.BitDelete},
				})
			})
		})

		t.Run("with splits", func(t *testing.T) {
			runBadgerTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				// l1 -> l2
				// l1 层表; l2 多个表;
				// 1.sst[a-z]
				// 2.sst[...]  3.sst[...] 4.sst[...] 5.sst[...] ...
				// key 区间: A-Z
				l1 := []keyValVersion{
					{"A", "bar", 3, common.BitDelete},
					{"X", "bar", 3, common.BitDelete},
				}

				// key 区间: A-E
				l21 := []keyValVersion{
					{"A", "bar", 2, 0},
					{"E", "bar", 2, 0},
				}
				// key 区间: F-J
				l22 := []keyValVersion{
					{"F", "bar", 2, 0},
					{"J", "bar", 2, 0},
				}
				l23 := []keyValVersion{
					{"K", "bar", 2, 0},
					{"O", "bar", 2, 0},
				}
				l24 := []keyValVersion{
					{"P", "bar", 2, 0},
					{"T", "bar", 2, 0},
				}

				l25 := []keyValVersion{
					{"U", "bar", 2, 0},
					{"Z", "bar", 2, 0},
				}

				l3 := []keyValVersion{{"fooz", "baz", 1, 0}}

				createAndSetLevel(lsm, l1, 1)  // 2.sst
				createAndSetLevel(lsm, l21, 2) // 3.sst
				createAndSetLevel(lsm, l22, 2) // 4
				createAndSetLevel(lsm, l23, 2) // 5
				createAndSetLevel(lsm, l24, 2) // 6
				createAndSetLevel(lsm, l25, 2) // 7
				createAndSetLevel(lsm, l3, 3)  // 8

				getAllAndCheck(t, lsm, []keyValVersion{
					{"A", "bar", 2, 0},
					{"A", "bar", 3, common.BitDelete},

					{"B", "bar", 2, 0},
					{"B", "bar", 3, common.BitDelete},

					{"C", "bar", 2, 0},
					{"C", "bar", 3, common.BitDelete},

					{"D", "bar", 2, 0},
					{"D", "bar", 3, common.BitDelete},

					{"E", "bar", 2, 0},
					{"E", "bar", 3, common.BitDelete},

					{"F", "bar", 2, 0},
					{"F", "bar", 3, common.BitDelete},

					{"G", "bar", 2, 0},
					{"G", "bar", 3, common.BitDelete},

					{"H", "bar", 2, 0},
					{"H", "bar", 3, common.BitDelete},

					{"I", "bar", 2, 0},
					{"I", "bar", 3, common.BitDelete},

					{"J", "bar", 2, 0},
					{"J", "bar", 3, common.BitDelete},

					{"K", "bar", 2, 0},
					{"K", "bar", 3, common.BitDelete},

					{"L", "bar", 2, 0},
					{"L", "bar", 3, common.BitDelete},

					{"M", "bar", 2, 0},
					{"M", "bar", 3, common.BitDelete},

					{"N", "bar", 2, 0},
					{"N", "bar", 3, common.BitDelete},

					{"O", "bar", 2, 0},
					{"O", "bar", 3, common.BitDelete},

					{"P", "bar", 2, 0},
					{"P", "bar", 3, common.BitDelete},

					{"Q", "bar", 2, 0},
					{"Q", "bar", 3, common.BitDelete},

					{"R", "bar", 2, 0},
					{"R", "bar", 3, common.BitDelete},

					{"S", "bar", 2, 0},
					{"S", "bar", 3, common.BitDelete},

					{"T", "bar", 2, 0},
					{"T", "bar", 3, common.BitDelete},

					{"U", "bar", 2, 0},
					{"U", "bar", 3, common.BitDelete},

					{"V", "bar", 2, 0},
					{"V", "bar", 3, common.BitDelete},

					{"W", "bar", 2, 0},
					{"W", "bar", 3, common.BitDelete},

					{"X", "bar", 2, 0},
					{"X", "bar", 3, common.BitDelete},

					{"Y", "bar", 2, 0},
					//{"Y", "bar", 3, common.BitDelete},

					{"Z", "bar", 2, 0},
					//{"Z", "bar", 3, common.BitDelete},

					{"fooz", "baz", 1, 0},
				})

				cdef := compactDef{
					thisLevel:  lsm.LevelManger.levelHandlers[1],
					nextLevel:  lsm.LevelManger.levelHandlers[2],
					thisTables: lsm.LevelManger.levelHandlers[1].tables,
					nextTables: lsm.LevelManger.levelHandlers[2].tables,
				}
				cdef.thisRange = getKeyRange(lsm.LevelManger.levelHandlers[1].tables...)
				cdef.nextRange = getKeyRange(lsm.LevelManger.levelHandlers[2].tables...)
				cdef.dst = lsm.LevelManger.levelTargets()
				cdef.dst.dstLevelId = 2
				require.NoError(t, lsm.LevelManger.runCompactDef(-1, 1, cdef))

				getAllAndCheck(t, lsm, []keyValVersion{
					{"A", "bar", 3, common.BitDelete},
					//{"A", "bar", 2, 0},

					{"B", "bar", 3, common.BitDelete},
					//{"B", "bar", 2, 0},

					{"C", "bar", 3, common.BitDelete},
					//{"C", "bar", 2, 0},

					{"D", "bar", 3, common.BitDelete},
					//{"D", "bar", 2, 0},

					{"E", "bar", 3, common.BitDelete},
					//{"E", "bar", 2, 0},

					{"F", "bar", 3, common.BitDelete},
					//{"F", "bar", 2, 0},

					{"G", "bar", 3, common.BitDelete},
					//{"G", "bar", 2, 0},

					{"H", "bar", 3, common.BitDelete},
					//{"H", "bar", 2, 0},

					{"I", "bar", 3, common.BitDelete},
					//{"I", "bar", 2, 0},

					{"J", "bar", 3, common.BitDelete},
					//{"J", "bar", 2, 0},

					{"K", "bar", 3, common.BitDelete},
					//{"K", "bar", 2, 0},

					{"L", "bar", 3, common.BitDelete},
					//{"L", "bar", 2, 0},

					{"M", "bar", 3, common.BitDelete},
					//{"M", "bar", 2, 0},

					{"N", "bar", 3, common.BitDelete},
					//{"N", "bar", 2, 0},

					{"O", "bar", 3, common.BitDelete},
					//{"O", "bar", 2, 0},

					{"P", "bar", 3, common.BitDelete},
					//{"P", "bar", 2, 0},

					{"Q", "bar", 3, common.BitDelete},
					//{"Q", "bar", 2, 0},

					{"R", "bar", 3, common.BitDelete},
					//{"R", "bar", 2, 0},

					{"S", "bar", 3, common.BitDelete},
					//{"S", "bar", 2, 0},

					{"T", "bar", 3, common.BitDelete},
					//{"T", "bar", 2, 0},

					{"U", "bar", 3, common.BitDelete},
					//{"U", "bar", 2, 0},

					{"V", "bar", 3, common.BitDelete},
					//{"V", "bar", 2, 0},

					{"W", "bar", 3, common.BitDelete},
					//{"W", "bar", 2, 0},

					{"X", "bar", 3, common.BitDelete},
					//{"X", "bar", 2, 0},

					//{"Y", "bar", 3, common.BitDelete},
					{"Y", "bar", 2, 0},

					//{"Z", "bar", 3, common.BitDelete},
					{"Z", "bar", 2, 0},

					{"fooz", "baz", 1, 0},
				})
			})
		})
	})
}

func getAllAndCheck(t *testing.T, lsm *LSM, expected []keyValVersion) {
	newLsmIterator := lsm.NewLsmIterator(&model.Options{IsAsc: true})
	it := NewMergeIterator(newLsmIterator, false)
	defer it.Close()
	i := 0
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item().Item
		item.Key = model.ParseKey(item.Key)
		//fmt.Printf("key:%s ,val:%s,varsion:%d, meta:%d\n", string(item.Key), string(item.Value), item.Version, item.Meta)
		require.Less(t, i, len(expected), "DB has more number of key than expected")
		expect := expected[i]
		require.Equal(t, expect.key, string(item.Key), "expected key: %s actual key: %s", expect.key, item.Key)
		require.Equal(t, expect.val, string(item.Value), "key: %s expected value: %s actual %s", item.Key, expect.val, item.Value)
		require.Equal(t, expect.version, int(item.Version), "key: %s expected version: %d actual %d", item.Key, expect.version, item.Version)
		require.Equal(t, expect.meta, item.Meta, "key: %s, version:%d, expected meta: %d meta %d", item.Key, item.Version, expect.meta, item.Meta)
		i++
	}
	require.Equal(t, len(expected), i, "keys examined should be equal to keys expected")
}

func runBadgerTest(t *testing.T, opts *Options, test func(t *testing.T, lsm *LSM)) {
	if opts == nil {
		opts = compactOptions
	}
	c := make(chan map[uint32]int64)
	compactOptions.DiscardStatsCh = &c
	clearDir(opts.WorkDir)
	lsm := NewLSM(opts, utils.NewCloser(1))
	defer lsm.Close()
	test(t, lsm)
}

// generateAlphabet 生成两个单字符之间的所有字符（包含边界）
// 参数要求必须是长度为1的字符串
func generateAlphabet(start, end string) ([]string, error) {
	// 验证输入有效性
	if len(start) != 1 || len(end) != 1 {
		return nil, errors.New("inputs must be single-character strings")
	}

	// 转换为rune类型
	s := []rune(start)[0]
	e := []rune(end)[0]

	// 自动处理顺序
	if s > e {
		s, e = e, s
	}

	// 生成字符序列
	result := make([]string, 0, e-s+1)
	for c := s; c <= e; c++ {
		result = append(result, string(c))
	}

	return result, nil
}
