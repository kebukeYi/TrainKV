package utils

import (
	"context"
	"sync"
	"sync/atomic"
)

// AtomicMark 是"索引与 Begin/Done 一一对应"场景下的水位实现(如 commitMark):
// 快路径(顺序完成)仅一次原子 CAS, 无 channel、无通知协程、无 goroutine 唤醒;
// 乱序完成与等待者注册才进入互斥锁慢路径。
//
// 与 LimitMark 的语义差异(使用方需知晓):
//   - Begin 只记录 lastIndex, 不参与连贯性判断; Done(x) 在 x == doneIndex+1 时可直接推进,
//     未完成(未 Done)的事务天然会挡住水位, 无需 Begin 计数;
//   - 不支持同一 index 的多次并发 Begin(共享索引场景请继续用 LimitMark, 如 startMark);
//   - 初始水位需用 SetDoneIndex 播种(等价于 LimitMark 初始 Done 跳越的语义)。
type AtomicMark struct {
	Name      string
	lastIndex atomic.Uint64 // Begin 的最大索引;
	doneIndex atomic.Uint64 // 连续完成的水位;

	mu        sync.Mutex // 以下仅慢路径使用;
	pending   u64Heap    // 乱序完成(已 Done 但不连续)的索引;
	waiters   map[uint64][]chan struct{}
	waiterCnt atomic.Int32 // 等待者数量, 供快路径跳过加锁;
	heapLen   atomic.Int32 // 堆内元素数, 供快路径跳过加锁;
}

func (m *AtomicMark) Init() {
	m.waiters = make(map[uint64][]chan struct{})
}

// Begin 仅更新最大索引; 不做任何跟踪与唤醒;
// begin的调用是具备 顺序性; 但是完成时的done()调用 不一定是顺序性;
func (m *AtomicMark) Begin(x uint64) {
	m.lastIndex.Store(x)
}

// SetDoneIndex 直接播种水位(Begin 与 Done 同时前移), 用于启动/重放后的初始化;
func (m *AtomicMark) SetDoneIndex(x uint64) {
	m.lastIndex.Store(x)
	m.doneIndex.Store(x)
}

func (m *AtomicMark) GetDoneIndex() uint64 {
	return m.doneIndex.Load()
}

func (m *AtomicMark) GetLastIndex() uint64 {
	return m.lastIndex.Load()
}

// begin的调用是具备 顺序性; 但是完成时的done()调用 不一定是顺序性;
// Done 标记索引完成: 顺序完成走 CAS 快路径, 乱序完成压堆等待吸收;
func (m *AtomicMark) Done(x uint64) {
	// 快路径: 期望顺序发生;
	for {
		// 当前最新已结束水位;
		d := m.doneIndex.Load()
		// x:7;  d:5;
		if x != d+1 {
			// 不一致时, 将 x:7 放入池中,等待 x:6的唤醒;
			break
		}
		// 顺序的话, 执行cas;
		if m.doneIndex.CompareAndSwap(d, x) {
			// 消费堆中下一个元素; absorb 可能把水位连推多个 index,
			// 因此必须用消费后的最终水位唤醒, 否则 (x, 新水位] 的等待者会漏唤醒;
			m.absorb()
			m.wakeWaiters(m.GetDoneIndex())
			return
		}
	}

	// 慢路径: 存在乱序发生;
	// d:5; heap: [7,8,9,10], 在等待6的到来;
	m.mu.Lock()
	m.pending.push(x)
	m.heapLen.Store(int32(len(m.pending)))
	m.mu.Unlock()

	// 压堆后必须再尝试吸收一次: 存在如下竞态窗口 —— 本协程读到 doneIndex 在快路径 CAS 之前,
	// 而 push 发生在快路径 absorb 之后(快路径当时看到堆为空), 该索引就永远留在堆里,
	// 水位停住且不会再有 Done 来推动 → 所有 WaitForIndexDone 死等.
	m.absorb()
	m.wakeWaiters(m.GetDoneIndex())
}

// 消费堆中紧跟着连续完成的索引; 堆为空时零开销返回;
func (m *AtomicMark) absorb() {
	if m.heapLen.Load() == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for len(m.pending) > 0 {
		d := m.doneIndex.Load()
		if m.pending[0] != d+1 {
			break
		}
		m.pending.pop()
		m.doneIndex.Store(d + 1)
	}
	m.heapLen.Store(int32(len(m.pending)))
}

// wakeWaiters 唤醒所有等待 index <= until 的协程; 无等待者时零开销返回;
func (m *AtomicMark) wakeWaiters(until uint64) {
	if m.waiterCnt.Load() == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for idx, chs := range m.waiters {
		if idx <= until {
			for _, ch := range chs {
				close(ch)
			}
			m.waiterCnt.Add(-int32(len(chs)))
			delete(m.waiters, idx)
		}
	}
}

// WaitForIndexDone 等待水位到达 index: 已到达则零开销返回, 否则注册等待者;
func (m *AtomicMark) WaitForIndexDone(ctx context.Context, index uint64) error {
	if m.doneIndex.Load() >= index {
		return nil
	}
	waitCh := make(chan struct{})

	m.mu.Lock()
	if m.doneIndex.Load() >= index { // 双检, 防止注册期间的推进造成丢唤醒;
		m.mu.Unlock()
		return nil
	}
	m.waiters[index] = append(m.waiters[index], waitCh)
	m.waiterCnt.Add(1)
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		// 超时/取消后 waiter 可能仍留在 map 中(与 LimitMark 行为一致);
		// 之后被 close 时无人接收, 无副作用;
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}
