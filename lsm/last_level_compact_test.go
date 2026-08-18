package lsm

// Lmax→Lmax (findMaxLevelTables) 路径测试:
// 在末层构造带 stale 数据 (删除标记) 的表, 回拨表龄, 期望 RunOnce 触发末层合并;
// 背景: 该路径要求末层表 stale ≥ LevelMaxStaleDataSize(10MB) 且表龄 ≥ 1h;

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/pb"
	"github.com/kebukeYi/TrainKV/v2/utils"
	"github.com/stretchr/testify/require"
)

func TestLastLevelCompaction(t *testing.T) {
	runKVTest(t, compactOptions, func(t *testing.T, lsm *LSM) {
		// ① 构造末层 stale 表: 45 万条删除标记 ≈ 10MB+ stale;
		builder := NewSSTBuilder(compactOptions)
		for i := 0; i < 450000; i++ {
			key := model.KeyWithTs([]byte(fmt.Sprintf("del-key-%d", i)), 1)
			e := model.NewEntry(key, nil)
			e.Meta = common.BitDelete
			builder.AddStaleKey(e)
		}
		fileName := utils.FileNameSSTable(compactOptions.WorkDir, lsm.LevelManger.NextFileID())
		tab, err := OpenTable(lsm.LevelManger, fileName, builder)
		require.NoError(t, err)
		require.GreaterOrEqual(t, int(tab.getStaleDataSize()), common.LevelMaxStaleDataSize,
			"stale data should exceed LevelMaxStaleDataSize")

		// ② 注册进 MANIFEST (否则压缩清理时会报 removes non-existing table) 并挂到末层;
		require.NoError(t, lsm.LevelManger.manifestFile.addChanges(
			[]*pb.ManifestChange{newCreateChange(tab.fid, 6)}))
		l6 := lsm.LevelManger.levelHandlers[6]
		l6.mux.Lock()
		l6.tables = append(l6.tables, tab)
		l6.addSizeLocked(tab)
		l6.mux.Unlock()

		// ③ 版本水位拉满 + 回拨表龄 (findMaxLevelTables 要求 ≥1h);
		lsm.LevelManger.txnDoneIndex.Store(math.MaxUint64)
		old := time.Now().Add(-2 * time.Hour)
		tab.sst.SetCreatedAt(&old)

		// ④ 触发压缩: 期望末层 stale 合并执行;
		ok := lsm.LevelManger.RunOnce(0)
		require.True(t, ok, "last-level compaction should run with stale data")
	})
}
