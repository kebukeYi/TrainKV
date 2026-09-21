package TrainKV

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/stretchr/testify/require"
)

// TestOpenWithEmptyWAL 覆盖"崩溃后残留空 WAL"的重开场景:
// 进程在 memtable 轮转后、新 WAL 尚未写入任何记录时崩溃, 会留下预分配但全零的 WAL;
// 若 recovery 对其做 Truncate(0), ftruncate 之后的 mremap(new_size=0) 返回 EINVAL,
// 库将永久无法重开 (本次 go-ycsb 大 value 基准中实际踩到)。
func TestOpenWithEmptyWAL(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "trainkv-regression-empty-wal")
	removeAll(dir)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	opt := lsm.GetDefaultOpt(dir)
	// 造一个预分配、全零的 WAL 文件, 等价于崩溃现场;
	f, err := os.Create(filepath.Join(dir, "00001.wal"))
	require.NoError(t, err)
	require.NoError(t, f.Truncate(opt.MemTableSize))
	require.NoError(t, f.Close())

	db, err, _ := Open(opt)
	require.NoError(t, err)
	require.NotNil(t, db)
	require.NoError(t, db.Close())
}

// TestReopenTinyValueThreshold 覆盖极小 value 阈值下的重开:
// ShouldWriteValueToLSM 用严格小于 (<), 阈值=1 时连 finTxn 的 commitTs 字符串(1~N 字节)
// 都会被判定进 vlog, WAL 里 finTxn 记录的 value 变成 ValuePtr;
// recovery2SkipList 依赖 finTxn 的 value 是明文 ts 做 ParseUint → ErrBadTxn, 库无法重开;
func TestReopenTinyValueThreshold(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "trainkv-regression-tiny-threshold")
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	opt.ValueThreshold = 1 // 全部 value 进 vlog;

	db, err, _ := Open(opt)
	require.NoError(t, err)
	for i := 0; i < 12; i++ { // commitTs 递增到两位数, 覆盖 1 字节/2 字节 fin 值;
		txn := db.NewTransaction(true)
		require.NoError(t, txn.Set([]byte(fmt.Sprintf("tk-%02d", i)), []byte("v")))
		_, err := txn.Commit()
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	reDB, err, _ := Open(opt)
	require.NoError(t, err)
	defer reDB.Close()

	txn := reDB.NewTransaction(false)
	entry, err := txn.Get([]byte("tk-00"))
	txn.Discard()
	require.NoError(t, err)
	require.Equal(t, []byte("v"), entry.Value)
}

// TestVlogRemapConcurrentRead 覆盖大 value 进 vlog 后的 mmap 扩容竞态:
// 追加写越过映射长度时 Truncate 会 mremap(MREMAP_MAYMOVE), 可能把映射搬到新地址;
// 读者持有 VLogFile.Lock.RLock 期间直接引用旧映射做 SafeCopy,
// 旧实现扩容不加写锁 → 读到已解除映射的地址(SIGSEGV, race detector 亦可直接命中)。
func TestVlogRemapConcurrentRead(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "trainkv-regression-vlog-remap")
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	opt.ValueThreshold = 64        // 64KB value 进 vlog, finTxn 的 ts 明文保持内联;
	opt.ValueLogFileSize = 1 << 20 // 1MB: 少量 64KB value 即可触发扩容 mremap;

	db, err, _ := Open(opt)
	require.NoError(t, err)

	const valSize = 64 << 10
	writeVal := func(k int) {
		txn := db.NewTransaction(true)
		require.NoError(t, txn.Set([]byte(fmt.Sprintf("k-%03d", k)), bytes.Repeat([]byte{byte(k)}, valSize)))
		_, err := txn.Commit()
		require.NoError(t, err)
	}

	for i := 0; i < 8; i++ {
		writeVal(i)
	}
	// 关库时活动 vlog 会被截断到实际写入长度, 重开后映射紧贴数据末尾,
	// 之后的第一次追加就会触发 Truncate(扩容) → mremap;
	require.NoError(t, db.Close())

	db, err, _ = Open(opt)
	require.NoError(t, err)
	defer db.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				txn := db.NewTransaction(false)
				_, _ = txn.Get([]byte("k-000")) // 读 vlog 值 → SafeCopy 整段拷贝;
				txn.Discard()
			}
		}()
	}

	for i := 8; i < 64; i++ {
		writeVal(i)
	}
	close(stop)
	wg.Wait()
}
