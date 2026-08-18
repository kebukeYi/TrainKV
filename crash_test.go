package TrainKV

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const crashEntryNum = 50

// TestCrashRecovery 模拟进程崩溃(kill -9)后的数据恢复;
// 子进程写入数据后不 Close 直接退出, 父进程重新打开数据库校验数据完整性;
func TestCrashRecovery(t *testing.T) {
	t.Run("clean-crash", func(t *testing.T) {
		runCrashRecovery(t, "/usr/golanddata/trainkv/crash_clean", false)
	})
	t.Run("torn-wal-tail", func(t *testing.T) {
		runCrashRecovery(t, "/usr/golanddata/trainkv/crash_torn", true)
	})
}

func runCrashRecovery(t *testing.T, dir string, corruptWal bool) {
	removeAll(dir)

	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecoveryChild$")
	cmd.Env = append(os.Environ(), "TRAINKV_CRASH_CHILD=1", "TRAINKV_CRASH_DIR="+dir)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "crash child failed: %s", out)

	if corruptWal {
		corruptWalTail(t, dir)
	}

	opt := lsm.GetDefaultOpt(dir)
	reDB, err, _ := Open(opt)
	require.NoError(t, err)
	defer reDB.Close()

	for i := 0; i < crashEntryNum; i++ {
		key := []byte(fmt.Sprintf("crash-key-%d", i))
		txn := reDB.NewTransaction(false)
		entry, err := txn.Get(key)
		txn.Discard()
		require.NoError(t, err, "key %s not recovered", key)
		require.NotNil(t, entry)
		assert.Equal(t, fmt.Sprintf("crash-value-%d", i), string(entry.Value))
	}
}

// TestCrashRecoveryChild 在独立进程中写入数据后直接 os.Exit, 模拟掉电/kill -9;
func TestCrashRecoveryChild(t *testing.T) {
	if os.Getenv("TRAINKV_CRASH_CHILD") != "1" {
		t.Skip("only runs as crash child process")
	}
	dir := os.Getenv("TRAINKV_CRASH_DIR")
	require.NotEmpty(t, dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, _ := Open(opt)
	require.NoError(t, err)

	txn := db.NewTransaction(true)
	for i := 0; i < crashEntryNum; i++ {
		key := []byte(fmt.Sprintf("crash-key-%d", i))
		value := []byte(fmt.Sprintf("crash-value-%d", i))
		require.NoError(t, txn.Set(key, value))
	}
	_, err = txn.Commit()
	require.NoError(t, err)
	// 不调用 db.Close, 直接退出进程, 模拟崩溃;
	os.Exit(0)
}

// corruptWalTail 在 wal 文件"有效数据结束/零填充区开始"的边界处覆盖垃圾字节,
// 模拟掉电时最后一条记录被撕裂(半写)的场景;
func corruptWalTail(t *testing.T, dir string) {
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".wal") {
			continue
		}
		path := filepath.Join(dir, f.Name())
		buf, err := os.ReadFile(path)
		require.NoError(t, err)

		dataEnd := -1
		for i := 0; i+64 <= len(buf); i++ {
			if allZeroBytes(buf[i : i+64]) {
				dataEnd = i
				break
			}
		}
		require.NotEqual(t, -1, dataEnd, "wal file %s has no zero-fill region", path)
		require.Greater(t, dataEnd, 0, "wal file %s has no valid data", path)

		// 覆盖边界处 12 个字节, 制造一条撕裂记录;
		garbage := make([]byte, 12)
		for i := range garbage {
			garbage[i] = 0xDE
		}
		err = os.WriteFile(path, append(buf[:dataEnd], append(garbage, buf[dataEnd+12:]...)...), 0666)
		require.NoError(t, err)
	}
}

func allZeroBytes(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}
