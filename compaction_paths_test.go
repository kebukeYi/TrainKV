package TrainKV

// 三条压缩路径的专项端到端测试:
// 1. L0→L0   : 小表自合并 (findTablesL0ToL0: L0 得分<1 + 4 张小表 + 表龄≥10s);
// 2. L0→Lmax : L0 直达末层 (findTablesL0ToDstLevel: L0 得分≥1);
// 3. Lmax→Lmax: 末层 stale 合并 (findMaxLevelTables, 见 lsm 包测试);

import (
	"testing"
	"time"

	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/stretchr/testify/require"
)

func waitL0Count(t *testing.T, db *TrainKV, n int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables()) >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("L0 tables did not reach %d", n)
}

// writeBatchWithRotate 写 count 个 key (从 base 起) 后轮转一次;
func writeBatchWithRotate(t *testing.T, db *TrainKV, base, count int, val []byte) {
	t.Helper()
	txn := db.NewTransaction(true)
	for j := 0; j < count; j++ {
		if err := txn.Set(gcGetKey(base+j), val); err != nil {
			t.Fatal(err)
		}
		if j%500 == 0 {
			if _, err := txn.Commit(); err != nil {
				t.Fatal(err)
			}
			txn = db.NewTransaction(true)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	db.Lsm.Rotate()
}

// TestCompactionL0ToL0 L0→L0 自合并: 4 张互不重叠的小表 (得分 0.8<1 → 走 L0→L0),
// 合并成 1 张, 数据完整;
func TestCompactionL0ToL0(t *testing.T) {
	dir := t.TempDir()
	opt := lsm.GetDefaultOpt(dir)
	db, _, callBack := Open(opt)
	defer func() { _ = db.Close(); _ = callBack() }()

	val := make([]byte, 512)
	const perBatch = 2000
	for i := 0; i < 4; i++ {
		writeBatchWithRotate(t, db, i*perBatch, perBatch, val)
	}
	waitL0Count(t, db, 4)
	l0Before := len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables())
	require.Equal(t, 4, l0Before)

	// L0→L0 要求表龄 ≥ 10s (createdAt 来自文件 mtime, 缓存于打开时);
	time.Sleep(11 * time.Second)

	ok := db.Lsm.LevelManger.RunOnce(0)
	require.True(t, ok, "L0→L0 compaction should run")
	l0After := len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables())
	t.Logf("L0 tables: %d -> %d", l0Before, l0After)
	require.Less(t, l0After, l0Before, "L0 tables should be merged")

	// 数据完整性: 全部读回;
	rtxn := db.NewTransaction(false)
	defer rtxn.Discard()
	for i := 0; i < 4*perBatch; i++ {
		e, err := rtxn.Get(gcGetKey(i))
		require.NoError(t, err, "key %d should exist", i)
		require.Equal(t, string(val), string(e.Value), "key %d value mismatch", i)
	}
}

// TestCompactionL0ToLmax L0→Lmax: 5 张 L0 表 (得分 1.0) 直达末层 (小库 base level=L6),
// 数据完整;
func TestCompactionL0ToLmax(t *testing.T) {
	dir := t.TempDir()
	opt := lsm.GetDefaultOpt(dir)
	db, _, callBack := Open(opt)
	defer func() { _ = db.Close(); _ = callBack() }()

	val := make([]byte, 512)
	const perBatch = 2000
	for i := 0; i < 5; i++ {
		writeBatchWithRotate(t, db, i*perBatch, perBatch, val)
	}
	waitL0Count(t, db, 5)

	ok := db.Lsm.LevelManger.RunOnce(0)
	require.True(t, ok, "L0→Lmax compaction should run")
	l0After := len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables())
	// 小库的 base level = 末层 (L6);
	l6After := len(db.Lsm.LevelManger.GetLevelHandler(6).GetTables())
	t.Logf("L0 tables after=%d, L6 tables=%d", l0After, l6After)
	require.Zero(t, l0After, "L0 should be empty after L0→Lmax")
	require.Greater(t, l6After, 0, "Lmax should have tables after L0→Lmax")

	// 数据完整性: 全部读回;
	rtxn := db.NewTransaction(false)
	defer rtxn.Discard()
	for i := 0; i < 5*perBatch; i++ {
		e, err := rtxn.Get(gcGetKey(i))
		require.NoError(t, err, "key %d should exist", i)
		require.Equal(t, string(val), string(e.Value), "key %d value mismatch", i)
	}
}
