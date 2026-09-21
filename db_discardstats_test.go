package TrainKV

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/stretchr/testify/require"
)

// TestDiscardStatsSurvivesReopen 覆盖两条相邻的"重开后库不可用"缺陷:
//
//  1. vlog.sendDiscardStats 里把 db.get 已解引用过的 value 又当成 ValuePtr 解析一次:
//     db.get 返回的 value 是正文(统计表 JSON), 但 Meta 上的 BitValuePointer 位仍保留,
//     于是 JSON 的前 12 字节被解成一个不存在的 fid → #getVlogFileLocked not found →
//     vlog.Open panic("Failed to populate discard stats") → 库无法重开;
//
//  2. 统计记录以 common.MaxTs(MaxUint64) 作 ts 哨兵, 重放后被算进 maxVersion(MaxVersion
//     与 memTable/SST 的 maxVersion 维护), 使 nextTxnTs 溢出归 0; 之后新事务的
//     startTs/commitTs 退回极小值、与既有版本号重复, 水位不变量被打破
//     (txn.go 的 commitTs >= lastCleanupTs 断言失败 → log.Fatalf), 库能打开但一写就崩;
//
// 两种取值方式(统计表落 LSM / 落 vlog)都必须满足: 重开不 panic、统计能回填、发号不回退、重开后能继续读写;
func TestDiscardStatsSurvivesReopen(t *testing.T) {
	cases := []struct {
		name    string           // 统计表 value 的存放方式;
		payload []byte           // 统计表 JSON;
		want    map[uint32]int64 // 重开后应回填的统计 (fid -> 失效字节数);
	}{
		{"valueInlineInLsm", []byte(`{"1":1}`), map[uint32]int64{1: 1}},
		{ // 真实统计表通常远超 ValueThreshold → value 落 vlog, meta 带 BitValuePointer;
			"valueInVlog", []byte(`{"1":65664,"2":65664,"3":65664,"4":65664}`),
			map[uint32]int64{1: 65664, 2: 65664, 3: 65664, 4: 65664},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opt := lsm.GetDefaultOpt(dir)
			opt.ValueThreshold = 32 // 让 valueInVlog 的 value 落 vlog;

			db, err, callBack := Open(opt)
			require.NoError(t, err)
			// 先写一个大 value: 磁盘上有 vlog 文件时 vlog.Open 才会走到统计回填;
			txn := db.NewTransaction(true)
			require.NoError(t, txn.Set([]byte("big-key"), []byte(strings.Repeat("v", 1000))))
			_, err = txn.Commit()
			require.NoError(t, err)

			// 等价于 processDiscardStats 的写入: 一条 Meta=0 的记录送进写通道 → WAL;
			entry := &model.Entry{
				Key:   model.KeyWithTs([]byte(common.VlogFileDiscardStatsKey), common.MaxTs),
				Value: tc.payload,
			}
			req, err := db.SendToWriteCh([]*model.Entry{entry})
			require.NoError(t, err)
			require.NoError(t, req.Wait())

			// 哨兵 ts 不能污染版本统计;
			userMaxTs := db.MaxVersion()
			require.NotEqual(t, uint64(math.MaxUint64), userMaxTs, "哨兵 ts 被算进了 MaxVersion")
			require.NoError(t, db.Close()) // close(false): 当前 WAL 保留, 供重开重放;
			_ = callBack()

			// 重开: 重放残留 WAL 里的 discard-stats 记录 + vlog.Open 回填统计;
			reDB, err, callBack2 := Open(opt)
			require.NoError(t, err)
			defer callBack2()

			// ① 重开后发号不回退(缺陷 2);
			require.Equal(t, userMaxTs, reDB.MaxVersion())

			// ② 统计由 handleDiscardStats 异步合并, 轮询等待回填完成(缺陷 1);
			require.Eventually(t, func() bool {
				reDB.vlog.VLogFileDisCardStaInfo.mux.RLock()
				defer reDB.vlog.VLogFileDisCardStaInfo.mux.RUnlock()
				for fid, size := range tc.want {
					if reDB.vlog.VLogFileDisCardStaInfo.FileMap[fid] != size {
						return false
					}
				}
				return true
			}, 3*time.Second, 10*time.Millisecond, "discard stats not restored after reopen")

			// ③ 重开后能继续读写, 且新 commitTs 严格大于既有版本;
			tw := reDB.NewTransaction(true)
			require.NoError(t, tw.Set([]byte("after-reopen"), []byte("v2")))
			commitTs, err := tw.Commit()
			require.NoError(t, err)
			require.Greater(t, commitTs, userMaxTs, "重开后 commitTs 与既有版本重复")

			rtxn := reDB.NewTransaction(false)
			e, err := rtxn.Get([]byte("big-key"))
			require.NoError(t, err)
			require.Len(t, e.Value, 1000)
			rtxn.Discard()

			require.NoError(t, reDB.Close())
		})
	}
}
