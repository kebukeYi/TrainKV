package utils

import (
	"container/heap"
	"context"
	"sync/atomic"
)

type minHeap []uint64

func (h minHeap) Len() int {
	return len(h)
}
func (h minHeap) Less(i, j int) bool {
	return h[i] < h[j]
}
func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(uint64))
}
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	ret := old[n-1]
	*h = old[0 : n-1]
	return ret
}

type mark struct {
	index   uint64
	done    bool
	waiter  chan struct{}
	indices []uint64 // 同一批,多个 index
}

type LimitMark struct {
	Name      string
	markCh    chan mark
	lastIndex atomic.Uint64 // 目前最新的索引(其结束需等待前面的索引结束通知);
	doneIndex atomic.Uint64 // 已经结束的索引;
}

func (lm *LimitMark) Init(closer *Closer, doneIndexCh chan uint64) {
	lm.markCh = make(chan mark, 100)
	// 流水线似,处理索引;
	go lm.processOn(closer, doneIndexCh)
}

func (lm *LimitMark) Begin(x uint64) {
	lm.lastIndex.Store(x)
	lm.markCh <- mark{
		index: x,
		done:  false,
	}
}

func (lm *LimitMark) Done(x uint64) {
	lm.markCh <- mark{
		index: x,
		done:  true,
	}
}

func (lm *LimitMark) SetDoneIndex(x uint64) {
	lm.doneIndex.Store(x)
}

func (lm *LimitMark) GetDoneIndex() uint64 {
	return lm.doneIndex.Load()
}

func (lm *LimitMark) GetLastIndex() uint64 {
	return lm.lastIndex.Load()
}

func (lm *LimitMark) WaitForIndexDone(ctx context.Context, index uint64) error {
	// 不能结束就等待结束, 并把自己装填在管道中,等待被消费;
	doneIndex := lm.GetDoneIndex()
	if doneIndex >= index {
		return nil
	}
	waitCh := make(chan struct{})
	lm.markCh <- mark{
		index:  index,
		waiter: waitCh,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

func (lm *LimitMark) processOn(closer *Closer, doneIndexCh chan uint64) {
	defer closer.Done()

	var minHeap minHeap
	heap.Init(&minHeap)

	// 每个 index 的 begin 数减 done 数;
	// < index, dones >
	indexDoneNum := make(map[uint64]int)
	// < index, chan[] > 等待此 index 结束的多个协程;
	indexWaiters := make(map[uint64][]chan struct{})

	processOne := func(index uint64, done bool) {
		// 1. 维护更新每个水位的跟踪值;
		prev, ok := indexDoneNum[index]
		if !ok {
			heap.Push(&minHeap, index)
		}
		delta := 1 // 默认是 begin, done = false,所以是 +1;
		if done {
			delta = -1 // done = true; 结束了 就-1;
		}
		indexDoneNum[index] = prev + delta

		// 2. 健全判断机制;
		doneIndex := lm.GetDoneIndex()
		if doneIndex > index {
			AssertTruef(false, "Name: %s doneUntil: %d. Index: %d", lm.Name, doneIndex, index)
		}

		// 3.循环,尝试更新水位值;
		curIndex := doneIndex
		loops := 0 // 循环几次, 就是 pop出几个index;
		// 循环算出 最小done的index;
		for len(minHeap) > 0 {
			minIndex := minHeap[0]
			dones := indexDoneNum[minIndex]
			if dones > 0 {
				// 堆中最小的,都没结束,后面的就更不会结束了;
				break
			}
			// dones<=0
			heap.Pop(&minHeap)
			delete(indexDoneNum, minIndex)
			curIndex = minIndex
			loops++
		}

		// 4. 判断水位值是否更新;
		// 不相等, 就相当于 index 水位进度更新了;
		if curIndex != doneIndex {
			swapped := lm.doneIndex.CompareAndSwap(doneIndex, curIndex)
			AssertTrue(swapped)
			// 通知 compactor , 这个活跃读事务终于结束了, 可以进行数据清理了;
			if doneIndexCh != nil {
				go func() {
					doneIndexCh <- curIndex
				}()
			}
		}

		notifyAndRemove := func(index uint64, toNotify []chan struct{}) {
			for _, waiter := range toNotify {
				close(waiter)
			}
			delete(indexWaiters, index)
		}

		// 下面的 if-else 是遍历区间优化:
		// 分支 A(密集时):len(indexWaiters) >= curIndex - doneIndex,
		// 等待者多、推进区间小 → 直接遍历区间 [doneIndex+1, curIndex],对每个编号查 map;
		// 剩余等待的 >= 刚刚通知的几个 index;
		//                   ->
		// 1 2 3 4 5 6 7 8 9 10 11 12 13
		// d     c   w w w w w  w  w   w
		// d     c
		//    w w w w w w w w w w  w   w
		if uint64(len(indexWaiters)) >= curIndex-doneIndex {
			for i := doneIndex + 1; i <= curIndex; i++ {
				if waiters, ok := indexWaiters[i]; ok {
					notifyAndRemove(i, waiters)
				}
			}
		} else {
			// 分支 B(稀疏时):等待者少、推进区间大 → 遍历 map 本身,关闭所有 idx <= curIndex 的等待者
			// 剩余等待的 < 刚刚处理的几个 index;
			// d                 c w w w w w
			// len(indexWaiters) < curIndex - doneIndex
			for idx, waiters := range indexWaiters {
				if idx <= curIndex {
					notifyAndRemove(idx, waiters)
				}
			}
		}
	}

	for {
		select {
		case <-closer.CloseSignal:
			return
		case m := <-lm.markCh:
			// 只有一种情况需要等: NewTraction();
			if m.waiter != nil {
				if lm.GetDoneIndex() >= m.index {
					close(m.waiter)
				} else {
					if waiters, ok := indexWaiters[m.index]; ok {
						indexWaiters[m.index] = append(waiters, m.waiter)
					} else {
						indexWaiters[m.index] = []chan struct{}{m.waiter}
					}
				}
			} else {
				// Begin(index,false), Done(index,true);
				if m.index > 0 || (len(m.indices) == 0 && m.index == 0) {
					// m.index >= 0 会进入此逻辑;
					processOne(m.index, m.done)
				}
				for _, index := range m.indices {
					processOne(index, m.done)
				}
			}
		}
	}
}
