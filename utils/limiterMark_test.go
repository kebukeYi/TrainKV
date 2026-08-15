package utils

import (
	"context"
	"testing"
	"time"
)

// utils/limiterMark_test.go
func TestLimitMarkWaiterNotifyRace(t *testing.T) {
	lm := &LimitMark{Name: "t"}
	lm.Init(NewCloser(1), nil)
	defer lm.SetDoneIndex(0) // 仅示意

	lm.Begin(1)
	lm.Begin(2) // 两个在途提交
	done := make(chan struct{})
	go func() {
		_ = lm.WaitForIndexDone(context.Background(), 1) // 等待者 @1
		close(done)
	}()

	time.Sleep(50 * time.Millisecond) // 确保 waiter 已注册
	lm.Done(2)                        // 先完成 2: min=1 未完成 → 不推进
	lm.Done(1)                        // 一次 pop 1、2,跨度 2 > waiter 1 → else 分支
	<-done                            // waiter 被 close, 但 indexWaiters[1] 残留 (已 close 的 chan)

	// 第二轮制造同样形状 → 遍历 map 时对残留条目二次 close → PANIC
	lm.Begin(3)
	lm.Begin(4)
	go func() { _ = lm.WaitForIndexDone(context.Background(), 3) }()
	time.Sleep(50 * time.Millisecond)
	lm.Done(4)
	lm.Done(3) // 期望:panic: close of closed channel
	time.Sleep(50 * time.Millisecond)
}
