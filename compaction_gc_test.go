package TrainKV

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/stretchr/testify/require"
)

var gcKeyPrefix = []byte("test_key_")

func gcGetKey(n int) []byte {
	key := make([]byte, 0, len(gcKeyPrefix)+9)
	key = append(key, gcKeyPrefix...)
	return strconv.AppendInt(key, int64(n), 10)
}

// TestDBAutoCompaction DB 级 compaction 端到端正确性:
// 写入 + 更新 + 删除 → 多个 L0 表 → 执行自动 compaction 的同一路径 (runOnce)
// → 验证全部数据读回正确 (更新取新值、删除报 not found)、L0 表被合并;
func TestDBAutoCompaction(t *testing.T) {
	dir := t.TempDir()
	opt := lsm.GetDefaultOpt(dir)
	db, _, callBack := Open(opt)
	defer func() { _ = db.Close(); _ = callBack() }()

	const (
		total   = 60000
		updFrom = 10000
		updTo   = 20000
		delFrom = 40000
		delTo   = 45000
	)
	// ① 写入 total 个 key (512B value, LSM 驻留), 更新 [updFrom,updTo), 删除 [delFrom,delTo);
	txn := db.NewTransaction(true)
	for i := 0; i < total; i++ {
		val := []byte(fmt.Sprintf("val-%d", i))
		if i >= updFrom && i < updTo {
			val = []byte(fmt.Sprintf("new-%d", i))
		}
		if err := txn.Set(gcGetKey(i), val); err != nil {
			t.Fatal(err)
		}
		if i%500 == 0 {
			if _, err := txn.Commit(); err != nil {
				t.Fatal(err)
			}
			txn = db.NewTransaction(true)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	for i := delFrom; i < delTo; i++ {
		dtxn := db.NewTransaction(true)
		if err := dtxn.Delete(gcGetKey(i)); err != nil {
			t.Fatal(err)
		}
		if _, err := dtxn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// ② 制造 5 个 L0 表: 每批写入后轮转 (空表轮转会被跳过, 必须每批都有数据);
	//    注意: L0 压缩触发条件为 表数 ≥ NumLevelZeroTables(5);
	chunk := 5000
	for i := 0; i < 5; i++ {
		txn := db.NewTransaction(true)
		for j := 0; j < chunk; j++ {
			k := total + i*chunk + j
			if err := txn.Set(gcGetKey(k), []byte("extra")); err != nil {
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
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables()) >= 5 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	l0Before := len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables())
	require.GreaterOrEqual(t, l0Before, 5, "L0 tables should be flushed")

	// ③ 执行自动 compaction 的同一路径 (compactor 协程 50s ticker 调用的就是它);
	//    期间并发读取, 验证 compaction 与读并发安全;
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rtxn := db.NewTransaction(false)
		defer rtxn.Discard()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := rtxn.Get(gcGetKey(30000)); err != nil {
					t.Errorf("concurrent read failed: %v", err)
					return
				}
			}
		}
	}()
	db.Lsm.LevelManger.RunOnce(0)
	close(stop)
	wg.Wait()

	// ④ 验证: 更新取新值, 未动取原值, 删除报 not found; 追加批次的数据也要可读;
	rtxn := db.NewTransaction(false)
	defer rtxn.Discard()
	totalAll := total + 5*chunk
	for i := 0; i < totalAll; i++ {
		if i >= total {
			e, err := rtxn.Get(gcGetKey(i))
			require.NoError(t, err, "extra key %d should exist", i)
			require.Equal(t, "extra", string(e.Value), "extra key %d value mismatch", i)
			continue
		}
		e, err := rtxn.Get(gcGetKey(i))
		if i >= delFrom && i < delTo {
			require.ErrorIs(t, err, common.ErrKeyNotFound, "deleted key %d should be gone", i)
			continue
		}
		require.NoError(t, err, "key %d should exist", i)
		want := fmt.Sprintf("val-%d", i)
		if i >= updFrom && i < updTo {
			want = fmt.Sprintf("new-%d", i)
		}
		require.Equal(t, want, string(e.Value), "key %d value mismatch", i)
	}

	// ⑤ L0 表应被合并 (小库的 base level 由 levelTargets 决定, 可能直达 L6);
	l0After := len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables())
	t.Logf("L0 tables: %d -> %d", l0Before, l0After)
	require.Less(t, l0After, l0Before, "L0 tables should be compacted")
}

// TestValueLogGCFull vlog GC 全链路正确性:
// 大 value 写入 + 更新 + 删除 → compaction 生成 discard 统计 → RunValueLogGC 挑选并重写
// → 验证存活 key 值正确、删除 key 消失、旧 vlog 文件被删除、重启后数据完好、并发读安全;
func TestValueLogGCFull(t *testing.T) {
	dir := t.TempDir()
	opt := lsm.GetDefaultOpt(dir)
	opt.ValueThreshold = 10          // 值进 vlog;
	opt.ValueLogFileSize = 256 << 10 // 256KB/文件, 快速轮转出多个 vlog 文件;
	opt.ValueLogMaxEntries = 100000
	db, _, callBack := Open(opt)
	defer func() { _ = db.Close(); _ = callBack() }()

	const (
		n        = 100 // 初始写入;
		updFrom  = 30  // 更新 [30,60) → 旧 vlog 条目变死;
		updTo    = 60
		delFrom  = 80 // 删除 [80,100) → vlog 条目变死;
		delTo    = 100
	)
	val := make([]byte, 4<<10) // 4KB > 阈值 10B;
	// 分 5 批写入并轮转, 制造 5 个 L0 表 (L0 压缩触发条件: 表数 ≥ NumLevelZeroTables);
	for batch := 0; batch < 5; batch++ {
		txn := db.NewTransaction(true)
		from, to := batch*n/5, (batch+1)*n/5
		if batch == 4 {
			to = n
		}
		for i := from; i < to; i++ {
			if err := txn.Set(gcGetKey(i), val); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
		db.Lsm.Rotate()
	}
	for i := updFrom; i < updTo; i++ {
		utxn := db.NewTransaction(true)
		if err := utxn.Set(gcGetKey(i), []byte("updated")); err != nil {
			t.Fatal(err)
		}
		if _, err := utxn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	for i := delFrom; i < delTo; i++ {
		dtxn := db.NewTransaction(true)
		if err := dtxn.Delete(gcGetKey(i)); err != nil {
			t.Fatal(err)
		}
		if _, err := dtxn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	// 更新/删除都写进了活跃 memtable, 必须轮转让它们进入 SST, 压缩才能对比新旧版本;
	db.Lsm.Rotate()

	// ① 把数据 flush 成 SST (5 批 + 最终更新/删除批 = 6 张表), 并执行一次 compaction 生成 vlog discard 统计;
	db.Lsm.Rotate()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables()) >= 6 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("discardTs=%d", db.GetTransactionManager().DiscardTs())
	ok := db.Lsm.LevelManger.RunOnce(0)
	t.Logf("RunOnce ok=%v L0=%d L1=%d", ok,
		len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables()),
		len(db.Lsm.LevelManger.GetLevelHandler(1).GetTables()))
	// ② 等待 discard 统计合并进 FileMap;
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		db.vlog.VLogFileDisCardStaInfo.mux.RLock()
		statsLen := len(db.vlog.VLogFileDisCardStaInfo.FileMap)
		db.vlog.VLogFileDisCardStaInfo.mux.RUnlock()
		if statsLen > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	db.vlog.VLogFileDisCardStaInfo.mux.RLock()
	t.Logf("FileMap=%v", db.vlog.VLogFileDisCardStaInfo.FileMap)
	db.vlog.VLogFileDisCardStaInfo.mux.RUnlock()

	// ③ GC 期间并发读;
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rtxn := db.NewTransaction(false)
		defer rtxn.Discard()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := rtxn.Get(gcGetKey(10)); err != nil {
					t.Errorf("concurrent read during GC failed: %v", err)
					return
				}
			}
		}
	}()
	beforeFiles := vlogFileNames(t, dir) // GC 前快照;
	err := db.RunValueLogGC(0.1)
	close(stop)
	wg.Wait()
	require.NoError(t, err, "RunValueLogGC should succeed (discard stats present)")

	// ④ 验证: 存活 key 值正确 (更新的取新值), 删除 key 消失;
	rtxn := db.NewTransaction(false)
	defer rtxn.Discard()
	for i := 0; i < n; i++ {
		e, err := rtxn.Get(gcGetKey(i))
		if i >= delFrom && i < delTo {
			require.ErrorIs(t, err, common.ErrKeyNotFound, "deleted key %d should be gone", i)
			continue
		}
		require.NoError(t, err, "key %d should exist after GC", i)
		want := string(val)
		if i >= updFrom && i < updTo {
			want = "updated"
		}
		require.Equal(t, want, string(e.Value), "key %d value mismatch after GC", i)
	}

	// ⑤ 被选中的旧 vlog 文件应被删除 (重写可能新建文件, 数量不一定减少);
	afterFiles := vlogFileNames(t, dir)
	t.Logf("vlog files before=%v after=%v", beforeFiles, afterFiles)
	anyDeleted := false
	for _, f := range beforeFiles {
		if !containsStr(afterFiles, f) {
			anyDeleted = true
			break
		}
	}
	require.True(t, anyDeleted, "picked vlog file should be deleted after GC")

	// ⑥ 重启后数据完好;
	require.NoError(t, db.Close())
	db2, err2, callBack2 := Open(opt)
	require.NoError(t, err2)
	defer func() { _ = db2.Close(); _ = callBack2() }()
	rtxn2 := db2.NewTransaction(false)
	defer rtxn2.Discard()
	for i := 0; i < n; i++ {
		if i >= delFrom && i < delTo {
			_, err := rtxn2.Get(gcGetKey(i))
			require.ErrorIs(t, err, common.ErrKeyNotFound, "deleted key %d should stay gone", i)
			continue
		}
		e, err := rtxn2.Get(gcGetKey(i))
		require.NoError(t, err, "key %d should exist after reopen", i)
		want := string(val)
		if i >= updFrom && i < updTo {
			want = "updated"
		}
		require.Equal(t, want, string(e.Value), "key %d value mismatch after reopen", i)
	}
}

func vlogFileCount(t *testing.T, dir string) int {
	t.Helper()
	return len(vlogFileNames(t, dir))
}

func containsStr(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func vlogFileNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".vlog") {
			names = append(names, e.Name())
		}
	}
	return names
}

