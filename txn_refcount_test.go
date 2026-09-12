package TrainKV

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/kebukeYi/TrainKV/v2/lsm"
)

// TestStartTsRefcount 锁住"发号引用计数"的生命周期不变量:
//  1. 同一 ts 上的并发事务共享一个计数器;
//  2. 事务跨过提交后, 旧 ts 计数退役进 retired, 新发号落到新 ts;
//  3. 旧 ts 的最后一个事务结束时清理 retired 并发布 Done, 水位推进到该 ts.
func TestStartTsRefcount(t *testing.T) {
	var mirror atomic.Uint64
	tm := NewTransactionManager(&lsm.Options{TxnDoneIndex: &mirror})
	defer tm.Stop()

	tm.initStartTs(100) // 发号 ts = 100
	tm.nextTxnTs = 101
	tm.commitMark.SetDoneIndex(100)

	// 1) 同一 ts 的多个事务共享一个计数器(纯读场景的核心);
	ts1 := tm.startTs(false)
	ts2 := tm.startTs(false)
	if ts1 != 100 || ts2 != 100 {
		t.Fatalf("startTs = %d/%d, want 100/100", ts1, ts2)
	}
	if got := tm.curCnt.Load().refs.Load(); got != 2 {
		t.Fatalf("curRefs = %d, want 2", got)
	}

	// 2) 事务跨过提交: 旧 ts 退役进 retired, 发号推进到新 ts
	//    (与 newCommitTs 的顺序一致: 先 nextTxnTs++, 再 retireStartTs(新 ts));
	tx1 := &Transaction{startTs: ts1}
	tm.nextTxnTs++
	tm.retireStartTs(101)
	if cur := tm.curCnt.Load(); cur == nil || cur.ts != 101 {
		t.Fatalf("curCnt.ts = %v, want 101", cur)
	}
	tm.refMu.Lock()
	_, retiredOK := tm.retired[100]
	tm.refMu.Unlock()
	if !retiredOK {
		t.Fatal("ts=100 仍有活跃事务, 退役后应进入 retired")
	}

	// 3) 新 ts 的发号不与旧计数混淆(先把 commitMark 水位推到 101, 模拟该提交已落地);
	tm.commitMark.SetDoneIndex(101)
	if ts3 := tm.startTs(false); ts3 != 101 {
		t.Fatalf("startTs = %d, want 101", ts3)
	}

	// 4) 旧 ts 的最后一个事务结束: 清理 retired, 发布 Done, 水位推进到 100;
	tm.doneStart(tx1)
	tm.doneStart(&Transaction{startTs: ts2})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && tm.startMark.GetDoneIndex() < 100 {
		time.Sleep(time.Millisecond)
	}
	if got := tm.startMark.GetDoneIndex(); got != 100 {
		t.Fatalf("startMark doneIndex = %d, want 100", got)
	}
	tm.refMu.Lock()
	left := len(tm.retired)
	tm.refMu.Unlock()
	if left != 0 {
		t.Fatalf("retired 未清理, 剩 %d 项", left)
	}
}
