package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/interfaces"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/stretchr/testify/require"
	"math"
	"testing"

	"github.com/kebukeYi/TrainKV/pb"
	"github.com/kebukeYi/TrainKV/utils"
)

var compactTestPath = "/usr/golanddata/trainkv/compact"

var compactOptions = &Options{
	WorkDir:            compactTestPath,
	MemTableSize:       10 << 10,  // 10KB; 64 << 20(64MB)
	WaitFlushMemTables: 1,         // 默认: 15;
	BlockSize:          4 * 1024,  // 4 * 1024
	BloomFalsePositive: 0.01,      // 误差率
	CacheNums:          10 * 1024, // 10240个

	ValueThreshold:      maxValueThreshold, // 1B; 1 << 20(1MB)
	ValueLogMaxEntries:  100,               // 1000000
	ValueLogFileSize:    1 << 29,           // 512MB; 1<<30-1(1GB);
	ValueLogFileMaxSize: math.MaxUint32,
	SyncWrites:          false,
	VerifyValueChecksum: false, // false
	DiscardStatsCh:      nil,

	MaxBatchCount: 100,
	MaxBatchSize:  10 << 20, // 10 << 20(10MB)

	NumCompactors:       3,       // 4
	BaseLevelSize:       8 << 20, //8MB; 10 << 20(10MB)
	LevelSizeMultiplier: 10,
	TableSizeMultiplier: 2,
	BaseTableSize:       2 << 20, // 2 << 20(2MB)
	NumLevelZeroTables:  5,
	MaxLevelNum:         common.MaxLevelNum,
	SSTKeyRangCheckNums: 5000,
	NumVersionsToKeep:   1,
	TxnDoneIndexCh:      nil,
	// Txn
	DetectConflicts: true,
}

func createEmptyTable(lsm *LSM) *Table {
	b := NewSSTBuilder(compactOptions)
	defer b.close()
	// Add one key so that we can open this table.
	entry := model.Entry{Key: model.KeyWithTs([]byte("foo"), uint64(1)), Value: []byte{}}
	b.Add(&entry, false)
	fileName := utils.FileNameSSTable(compactOptions.WorkDir, lsm.LevelManger.NextFileID())
	tab, _ := OpenTable(&LevelsManger{opt: &Options{BaseTableSize: compactOptions.BaseTableSize}}, fileName, b)
	return tab
}
func TestKeyCompare(t *testing.T) {
	key1 := model.KeyWithTs([]byte("foo"), uint64(1))
	key2 := model.KeyWithTs([]byte("fooz"), uint64(1))
	fmt.Printf("len(key1):%d \n", len(key1))               // 11
	fmt.Printf("len(key2):%d \n", len(key2))               // 12
	compare := bytes.Compare(key1, key2)                   // 1
	compareKeyWithTs := model.CompareKeyWithTs(key1, key2) // -1
	fmt.Printf("bytes.Compare(key1, key2) :%d \n", compare)
	fmt.Printf("model.CompareKeyWithTs(key1, key2) :%d \n", compareKeyWithTs)
}

func createAndSetLevel(lsm *LSM, td []keyValVersion, level int) {
	builder := NewSSTBuilder(compactOptions)
	defer builder.close()

	if len(td[0].key) == 1 {
		alphabet, err := generateAlphabet(td[0].key, td[len(td)-1].key)
		if err != nil {
			panic(err)
		}
		for _, c := range alphabet {
			key := model.KeyWithTs([]byte(c), uint64(td[0].version))
			e := model.NewEntry(key, []byte(td[0].val))
			e.Meta = td[0].meta
			builder.Add(e, false)
		}
	} else {
		// Add all keys and versions to the table.
		for _, item := range td {
			key := model.KeyWithTs([]byte(item.key), uint64(item.version))
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

		t.Run("same keys", func(t *testing.T) {
			runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				l0 := []keyValVersion{{"foo", "bar", 3, 0}}
				l1 := []keyValVersion{{"foo", "bar", 2, 0}}
				createAndSetLevel(lsm, l0, 0) // 0层的是高版本
				createAndSetLevel(lsm, l1, 1) // 1层的是低版本

				// level 0 和 level 0 有重叠;
				require.True(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 0))

				// level 0 和 level 1  有重叠;
				require.True(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 1))

				// level 0 和 level 2  无有重叠;
				// level 1 和 level 2  无有重叠;
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 2))
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[1].tables, 2))
				// 同上;
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 3))
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[1].tables, 3))

			})
		})

		t.Run("overlapping keys", func(t *testing.T) {
			runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				l0 := []keyValVersion{
					{"aa", "x", 1, 0},
					{"bb", "x", 1, 0},
					{"foo", "bar", 3, 0}}
				l1 := []keyValVersion{
					{"foo", "bar", 2, 0}}

				createAndSetLevel(lsm, l0, 0)
				createAndSetLevel(lsm, l1, 1)

				// level 0 和 level 0 1 有重叠;
				require.True(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 0))
				require.True(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[1].tables, 1))

				// level 1 和 level 0 有重叠; "foo" key;
				require.True(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 1))

				// level 2 3  和 level 0 无重叠;
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 2))
				require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 3))
			})
		})
	})

	t.Run("non-overlapping", func(t *testing.T) {
		runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
			l0 := []keyValVersion{
				{"aa", "x", 1, 0},
				{"bb", "x", 1, 0},
				{"cc", "bar", 3, 0}}
			l1 := []keyValVersion{
				{"foo", "bar", 2, 0}}
			createAndSetLevel(lsm, l0, 0)
			createAndSetLevel(lsm, l1, 1)

			// level 0 和 level 1 无重叠;
			require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 1))

			// level 2 3 和 level 0 无重叠;
			require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 2))
			require.False(t, lsm.LevelManger.checkOverlap(lsm.LevelManger.levelHandlers[0].tables, 3))
		})
	})
}

func TestCompaction(t *testing.T) {
	// 简单测试 l0中的数据合并到l1, 只保留相同key的最高版本单个数据;
	t.Run("level 0 to level 1", func(t *testing.T) {
		runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {

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
			// 起始数据状态

			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0},
				{"foo", "bar", 1, 0},
				{"fooz", "baz", 1, 0},
			})

			cdef := compactDef{
				thisLevel:  lsm.LevelManger.levelHandlers[0],
				nextLevel:  lsm.LevelManger.levelHandlers[1],
				thisTables: lsm.LevelManger.levelHandlers[0].tables,
				nextTables: lsm.LevelManger.levelHandlers[1].tables,
				dst:        lsm.LevelManger.levelTargets(), // 计算的 baseLevel 会直接到6层;
			}
			// 手动设置到 l1 层;
			cdef.dst.dstLevelId = 1
			lsm.LevelManger.txnDoneIndex.Store(math.MaxUint64)
			// 执行 l0 -> l1 层 合并计划;
			require.NoError(t, lsm.LevelManger.runCompactDef(-1, 0, cdef))
			// foo version 1, 2 将会被剔除掉, 只留下最高版本3 ;
			// 合并后的库中数据状态:
			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"fooz", "baz", 1, 0}})
		})
	})

	// 简单测试 l0中的数据合并到l1, 只保留相同key的最高版本单个数据;
	t.Run("level 0 to level 1 with duplicates", func(t *testing.T) {
		runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
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

			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 4, 0},
				{"foo", "bar", 3, 0},
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
			// 手动设置 现在的数据都是可合并状态;
			lsm.LevelManger.txnDoneIndex.Store(math.MaxUint64)
			require.NoError(t, lsm.LevelManger.runCompactDef(-1, 0, cdef))
			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 4, 0},
				{"fooz", "baz", 1, 0}})
		})
	})

	// 简单测试 l0中的数据合并到l1, 只保留相同key的最高版本单个数据;
	t.Run("level 0 to level 1 with lower overlap", func(t *testing.T) {
		runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
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
				{"foo", "bar", 4, 0},
				{"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0},
				{"foo", "bar", 1, 0},
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
			lsm.LevelManger.txnDoneIndex.Store(math.MaxUint64)
			require.NoError(t, lsm.LevelManger.runCompactDef(-1, 0, cdef))
			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 4, 0}, // 在level2层,l2 没有参与 l0->l1 的合并;
				{"foo", "bar", 1, 0},
				{"fooz", "baz", 1, 0},
			})
		})
	})

	// 简单测试 l1中的数据合并到l2, 只保留相同key的最高版本单个数据;
	t.Run("level 1 to level 2", func(t *testing.T) {
		runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
			l1 := []keyValVersion{
				{"foo", "bar", 3, 0},
				{"fooz", "baz", 1, 0}}
			l2 := []keyValVersion{
				{"foo", "bar", 2, 0}}

			createAndSetLevel(lsm, l1, 1)
			createAndSetLevel(lsm, l2, 2)

			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0},
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
			lsm.LevelManger.txnDoneIndex.Store(math.MaxUint64)
			require.NoError(t, lsm.LevelManger.runCompactDef(-1, 1, cdef))
			getAllAndCheck(t, lsm, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"fooz", "baz", 1, 0}})
		})
	})

	t.Run("level 1 to level 2 with delete", func(t *testing.T) {

		// 简单测试 l0中的数据合并到l1, 只保留相同key的最高版本单个数据,尽管是删除类型数据;
		t.Run("with overlap", func(t *testing.T) {
			runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				l1 := []keyValVersion{
					{"foo", "bar", 1, 0},
					{"fooz", "baz", 1, common.BitDelete}}
				l2 := []keyValVersion{
					{"foo", "bar", 2, 1},
				}
				l3 := []keyValVersion{
					{"foo", "bar", 3, common.BitDelete},
				}
				createAndSetLevel(lsm, l1, 1)
				createAndSetLevel(lsm, l2, 2)
				createAndSetLevel(lsm, l3, 3)

				getAllAndCheck(t, lsm, []keyValVersion{
					{"foo", "bar", 3, common.BitDelete},
					{"foo", "bar", 2, 1},
					{"foo", "bar", 1, 0},
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
				lsm.LevelManger.txnDoneIndex.Store(math.MaxUint64)
				require.NoError(t, lsm.LevelManger.runCompactDef(-1, 1, cdef))
				// 处在l1,l2 层中的 fooz 并没有和下层l3 有overlap, 按照常规下, 是会被清理掉的, 但是为什么没有清理掉?
				// 但是处在 l1,l2 层中的 foo 和l3层有重合, 因此 hasOverlap, 也就被置为 true; 因此属于是连带效应,没有被铲除;
				getAllAndCheck(t, lsm, []keyValVersion{
					{"foo", "bar", 3, common.BitDelete},  // 在l3层
					{"foo", "bar", 2, 1},                 // 在l2层
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
				// 数据全都被清理掉了;
				// 此时的 l2, l3 和 l4 都没有数据交集; 因此在l2->l3 合并时, 会将删除标记的数据跳过;
				getAllAndCheck(t, lsm, []keyValVersion{})
			})
		})

		t.Run("with bottom overlap", func(t *testing.T) {
			runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
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
					{"foo", "bar", 3, common.BitDelete},
					{"foo", "bar", 2, 0},
					{"fooz", "baz", 2, common.BitDelete},
					{"fooz", "baz", 1, 0},
				})
				// l1- > l2
				cdef := compactDef{
					thisLevel:  lsm.LevelManger.levelHandlers[1],
					nextLevel:  lsm.LevelManger.levelHandlers[2],
					thisTables: lsm.LevelManger.levelHandlers[1].tables,
					nextTables: lsm.LevelManger.levelHandlers[2].tables,
					dst:        lsm.LevelManger.levelTargets(),
				}
				cdef.dst.dstLevelId = 2
				lsm.LevelManger.txnDoneIndex.Store(math.MaxUint64)
				require.NoError(t, lsm.LevelManger.runCompactDef(-1, 1, cdef))
				// l1, l2 和 l3 有交集, 因此在合并过程中, 不能贸然把删除标记数据跳过;
				getAllAndCheck(t, lsm, []keyValVersion{
					{"foo", "bar", 3, common.BitDelete},
					{"fooz", "baz", 2, common.BitDelete},
					{"fooz", "baz", 1, 0},
				})
			})
		})

		t.Run("without overlap", func(t *testing.T) {
			runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
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
				// l1 -> l2
				cdef := compactDef{
					thisLevel:  lsm.LevelManger.levelHandlers[1],
					nextLevel:  lsm.LevelManger.levelHandlers[2],
					thisTables: lsm.LevelManger.levelHandlers[1].tables,
					nextTables: lsm.LevelManger.levelHandlers[2].tables,
					dst:        lsm.LevelManger.levelTargets(),
				}
				cdef.dst.dstLevelId = 2
				lsm.LevelManger.txnDoneIndex.Store(math.MaxUint64)
				require.NoError(t, lsm.LevelManger.runCompactDef(-1, 1, cdef))
				// 没有出现 重合, 删除标记跳过;
				getAllAndCheck(t, lsm, []keyValVersion{
					{"fooo", "barr", 2, 0},
				})
			})
		})

		t.Run("with splits", func(t *testing.T) {
			runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
				// l1 -> l2
				// l1 层表; l2 多个表;
				// 1.sst[a-z]
				// 2.sst[...]  3.sst[...] 4.sst[...] 5.sst[...] ...
				// key 区间: A-X
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
				// key 区间: K-O
				l23 := []keyValVersion{
					{"K", "bar", 2, 0},
					{"O", "bar", 2, 0},
				}
				// key 区间: P-T
				l24 := []keyValVersion{
					{"P", "bar", 2, 0},
					{"T", "bar", 2, 0},
				}
				// key 区间: U-Z
				l25 := []keyValVersion{
					{"U", "bar", 2, 0},
					{"Z", "bar", 2, 0},
				}

				l3 := []keyValVersion{{"fooz", "baz", 1, 0}}

				createAndSetLevel(lsm, l1, 1)  // 2.sst
				createAndSetLevel(lsm, l21, 2) // 3.sst
				createAndSetLevel(lsm, l22, 2) // 4.sst
				createAndSetLevel(lsm, l23, 2) // 5.sst
				createAndSetLevel(lsm, l24, 2) // 6.sst
				createAndSetLevel(lsm, l25, 2) // 7.sst
				createAndSetLevel(lsm, l3, 3)  // 8.sst

				getAllAndCheck(t, lsm, []keyValVersion{
					{"A", "bar", 3, common.BitDelete},
					{"A", "bar", 2, 0},

					{"B", "bar", 3, common.BitDelete},
					{"B", "bar", 2, 0},

					{"C", "bar", 3, common.BitDelete},
					{"C", "bar", 2, 0},

					{"D", "bar", 3, common.BitDelete},
					{"D", "bar", 2, 0},

					{"E", "bar", 3, common.BitDelete},
					{"E", "bar", 2, 0},

					{"F", "bar", 3, common.BitDelete},
					{"F", "bar", 2, 0},

					{"G", "bar", 3, common.BitDelete},
					{"G", "bar", 2, 0},

					{"H", "bar", 3, common.BitDelete},
					{"H", "bar", 2, 0},

					{"I", "bar", 3, common.BitDelete},
					{"I", "bar", 2, 0},

					{"J", "bar", 3, common.BitDelete},
					{"J", "bar", 2, 0},

					{"K", "bar", 3, common.BitDelete},
					{"K", "bar", 2, 0},

					{"L", "bar", 3, common.BitDelete},
					{"L", "bar", 2, 0},

					{"M", "bar", 3, common.BitDelete},
					{"M", "bar", 2, 0},

					{"N", "bar", 3, common.BitDelete},
					{"N", "bar", 2, 0},

					{"O", "bar", 3, common.BitDelete},
					{"O", "bar", 2, 0},

					{"P", "bar", 3, common.BitDelete},
					{"P", "bar", 2, 0},

					{"Q", "bar", 3, common.BitDelete},
					{"Q", "bar", 2, 0},

					{"R", "bar", 3, common.BitDelete},
					{"R", "bar", 2, 0},

					{"S", "bar", 3, common.BitDelete},
					{"S", "bar", 2, 0},

					{"T", "bar", 3, common.BitDelete},
					{"T", "bar", 2, 0},

					{"U", "bar", 3, common.BitDelete},
					{"U", "bar", 2, 0},

					{"V", "bar", 3, common.BitDelete},
					{"V", "bar", 2, 0},

					{"W", "bar", 3, common.BitDelete},
					{"W", "bar", 2, 0},

					{"X", "bar", 3, common.BitDelete},
					{"X", "bar", 2, 0},

					{"Y", "bar", 2, 0},

					{"Z", "bar", 2, 0},

					{"fooz", "baz", 1, 0},
				})
				// l1- > l2
				cdef := compactDef{
					thisLevel:  lsm.LevelManger.levelHandlers[1],
					nextLevel:  lsm.LevelManger.levelHandlers[2],
					thisTables: lsm.LevelManger.levelHandlers[1].tables,
					nextTables: lsm.LevelManger.levelHandlers[2].tables,
				}
				// 获取 l1 的 keyRange: A-X
				cdef.thisRange = getKeyRange(lsm.LevelManger.levelHandlers[1].tables...)
				// 获取 l2 的 keyRange: A-Z
				cdef.nextRange = getKeyRange(lsm.LevelManger.levelHandlers[2].tables...)
				cdef.dst = lsm.LevelManger.levelTargets()
				cdef.dst.dstLevelId = 2
				lsm.LevelManger.txnDoneIndex.Store(math.MaxUint64)
				require.NoError(t, lsm.LevelManger.runCompactDef(-1, 1, cdef))
				// 所有数据都 被 l2 覆盖了, 删除标记跳过;
				getAllAndCheck(t, lsm, []keyValVersion{
					{"Y", "bar", 2, 0},
					{"Z", "bar", 2, 0},
					{"fooz", "baz", 1, 0},
				})
			})
		})
	})
}

func getAllAndCheck(t *testing.T, lsm *LSM, expected []keyValVersion) {
	newLsmIterator := lsm.NewLsmIterator(&interfaces.Options{IsAsc: true, IsSetCache: false})
	it := NewMergingIterator(newLsmIterator, &interfaces.Options{IsAsc: true, IsSetCache: false})
	defer it.Close()
	i := 0
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item().Item
		item.Version = model.ParseTsVersion(item.Key)
		item.Key = model.ParseKey(item.Key)
		require.Less(t, i, len(expected), "DB has more number of key than expected")
		expect := expected[i]
		require.Equal(t, expect.key, string(item.Key), "expected key: %s actual key: %s", expect.key, item.Key)
		require.Equal(t, expect.val, string(item.Value), "key: %s expected value: %s actual %s", item.Key, expect.val, item.Value)
		require.Equal(t, expect.version, int(item.Version), "key: %s expected version: %d actual %d", item.Key, expect.version, item.Version)
		require.Equal(t, expect.meta, item.Meta, "key: %s, version:%d, expected meta: %d actual  %d", item.Key, item.Version, expect.meta, item.Meta)
		i++
	}
	require.Equal(t, len(expected), i, "keys examined should be equal to keys expected")
}

func runKVTest(t *testing.T, opts *Options, test func(t *testing.T, lsm *LSM)) {
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
