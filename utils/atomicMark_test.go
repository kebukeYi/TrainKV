package utils

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestAtomicMarkWakeAfterAbsorb 水位吸收连跳后, 所有 index <= 新水位的等待者都必须被唤醒.
//
// 回归用例: 快路径 CAS 成功后 absorb 可能把水位从 x 连推到 x+n,
// 若只用吸收前的 x 调 wakeWaiters, 落在 (x, x+n] 的等待者会漏唤醒,
// 且负载末尾没有新的 Done 来补救 → 事务永久阻塞
// (线上表现: 混合负载在 operationcount 即将跑完时 Count 冻结, 进程存活但 0 进度).
func TestAtomicMarkWakeAfterAbsorb(t *testing.T) {
	var m AtomicMark
	m.Init()

	// 乱序完成: 2 先进堆, 水位停在 0;
	m.Done(2)

	woke := make(chan struct{})
	go func() {
		if err := m.WaitForIndexDone(context.Background(), 2); err == nil {
			close(woke)
		}
	}()
	for i := 0; i < 100000 && m.waiterCnt.Load() == 0; i++ {
		runtime.Gosched() // 等等待者注册完成(同包测试可直接观察内部计数);
	}
	if m.waiterCnt.Load() == 0 {
		t.Fatal("waiter 未注册")
	}

	// 这一步 CAS(0,1) 后 absorb 会把水位直接推到 2;
	m.Done(1)

	select {
	case <-woke:
	case <-time.After(2 * time.Second):
		t.Fatalf("水位已到 %d, 但等待 index=2 的协程未被唤醒", m.GetDoneIndex())
	}
}

// TestAtomicMarkConcurrentDone 并发乱序 Done 时, 所有完成事件都必须被水位吸收.
//
// 回归用例: 曾经存在"压堆路径不再尝试吸收"的竞态 ——
// 压堆者读取 doneIndex 在快路径 CAS 之前, 而 push 发生在快路径 absorb 之后时,
// 该索引会永远留在 pending 堆里, 水位停住, 所有 WaitForIndexDone 死等
// (线上表现: 4 个 worker 全卡在 commitMark.WaitForIndexDone, CPU 0%).
func TestAtomicMarkConcurrentDone(t *testing.T) {
	const (
		rounds  = 20000
		workers = 16
	)

	for round := 0; round < rounds; round++ {
		var m AtomicMark
		m.Init()
		m.Begin(workers)

		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 1; i <= workers; i++ {
			wg.Add(1)
			go func(x uint64) {
				defer wg.Done()
				<-start
				runtime.Gosched() // 放大交错窗口;
				m.Done(x)
			}(uint64(i))
		}
		close(start)
		wg.Wait()

		if got := m.GetDoneIndex(); got != workers {
			t.Fatalf("round %d: doneIndex=%d, want %d (完成事件被卡在 pending 堆里未吸收)",
				round, got, workers)
		}
	}
}
