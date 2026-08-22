package benchmk

// flush→SST 读回完整性回归测试;
// 背景: block 缓存 (W-TinyLFU) 在 stageOne→stageTwo 晋升且 stageTwo 已满时,
// 会交换两个 list.Element 的 storeItem 内容, 而 Cache.get 返回交换前的旧 element,
// 导致 key 读到别人的 block (静默 miss, 表现为 flush 后 ~1.4% key 丢失);
// 修复: Cache.get 在 stage 操作后重新解析 key→element 映射;

import (
	"testing"

	"time"

	"github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/lsm"
)

func TestFlushReadIntegrity(t *testing.T) {
	clearDir(benchMarkDir)
	train, _, _ := TrainKV.Open(lsm.GetDefaultOpt(benchMarkDir))
	defer train.Close()

	const n = 100000
	txn := train.NewTransaction(true)
	for i := 0; i < n; i++ {
		if err := txn.Set(GetKey(i), GetValue()); err != nil {
			t.Fatal(err)
		}
		if i%500 == 0 {
			if _, err := txn.Commit(); err != nil {
				t.Fatal(err)
			}
			txn = train.NewTransaction(true)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// 强制轮转并等待 flush 完成;
	train.Lsm.Rotate()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(train.Lsm.LevelManger.GetLevelHandler(0).GetTables()) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 全量读回, 0 丢失;
	rtxn := train.NewTransaction(false)
	defer rtxn.Discard()
	misses := 0
	for i := 0; i < n; i++ {
		if _, err := rtxn.Get(GetKey(i)); err != nil {
			misses++
		}
	}
	if misses > 0 {
		t.Fatalf("%d/%d keys missing after flush (block cache bug?)", misses, n)
	}
}
