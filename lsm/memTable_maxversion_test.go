package lsm

import (
	"testing"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/model"
)

// TestMemTableMaxVersionIgnoresSentinel 内部元数据记录(vlog discard-stats)的 ts 是
// common.MaxTs 哨兵, 不能参与 maxVersion 统计: 否则它会成为本表的 maxVersion,
// 把真实的最大用户版本盖住, 重开后 nextTxnTs 退回极小值(与既有版本号重复);
func TestMemTableMaxVersionIgnoresSentinel(t *testing.T) {
	m, w := newRecoveryMemTable(t)
	defer m.close(true)

	discardKey := model.KeyWithTs([]byte(common.VlogFileDiscardStatsKey), common.MaxTs)
	writeWalEntries(w,
		&model.Entry{Key: model.KeyWithTs([]byte("k1"), 7), Value: []byte("v1")}, // 普通条目 → default 分支 → put;
		&model.Entry{Key: discardKey, Value: []byte(`{"1":1}`)},                  // 哨兵记录;
	)
	if _, err := m.recovery2SkipList(); err != nil {
		t.Fatalf("recovery2SkipList err: %v", err)
	}

	if m.maxVersion != 7 {
		t.Fatalf("maxVersion=%d, want 7 (哨兵 ts 被算进来了)", m.maxVersion)
	}
	// 哨兵记录本身仍须留在表中(重开时 sendDiscardStats 要按精确 ts 读到它);
	val := m.skipList.Get(discardKey)
	if val.Version == 0 && val.Value == nil {
		t.Fatalf("discard-stats 记录未写入 skiplist")
	}
}
