package utils

import (
	"container/heap"
	"context"
	"sync/atomic"
	"time"
)

type minHeap []uint64

func (h minHeap) Less(i, j int) bool {
	return h[i] < h[j]
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	ret := old[n-1]
	*h = old[0 : n-1]
	return ret
}
func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(uint64))
}
func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h minHeap) Len() int {
	return len(h)
}

type mark struct {
	index   uint64
	done    bool
	indices []uint64
	waiter  chan struct{}
}

type LimitMark struct {
	Name      string
	markCh    chan mark
	lastIndex atomic.Uint64 // 最新最近的索引;
	doneIndex atomic.Uint64
}

func (lm *LimitMark) Init(closer *Closer) {
	lm.markCh = make(chan mark, 100)
	go lm.processOn(closer)
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
	if index <= lm.GetDoneIndex() {
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

func (lm *LimitMark) processOn(closer *Closer) {
	defer closer.Done()

	var minHeap minHeap
	heap.Init(&minHeap)

	// < index, dones >
	indexDoneNum := make(map[uint64]int)
	// < index, chan[] > 等待此 index 结束的多个协程;
	indexWaiters := make(map[uint64][]chan struct{})

	processOne := func(index uint64, done bool) {
		time.Sleep(3 * time.Second)
		prev, ok := indexDoneNum[index]
		if !ok {
			heap.Push(&minHeap, index)
		}

		delta := 1
		if done {
			delta = -1
		}

		indexDoneNum[index] = prev + delta
		doneIndex := lm.GetDoneIndex()
		if doneIndex > index {
			AssertTruef(false, "Name: %s doneUntil: %d. Index: %d", lm.Name, doneIndex, index)
		}

		curIndex := doneIndex
		loops := 0

		for len(minHeap) > 0 {
			minIndex := minHeap[0]
			if dones := indexDoneNum[minIndex]; dones > 0 {
				break
			}
			heap.Pop(&minHeap)
			delete(indexDoneNum, minIndex)
			curIndex = minIndex
			loops++
		}

		if curIndex != doneIndex {
			swapped := lm.doneIndex.CompareAndSwap(doneIndex, curIndex)
			AssertTrue(swapped)
		}

		notifyAndRemove := func(index uint64, toNotify []chan struct{}) {
			for _, waiter := range toNotify {
				close(waiter)
			}
			delete(indexWaiters, index)
		}

		// 剩余等待的 >= 刚刚处理的几个 index;
		//                   ->
		//  doneUntil,,,,,,,,,,,,,,,,,,,,,,,,until
		//           ,,,index_1,,,,,,,,,,,,,,
		//           ,,,index_2,,
		//                       ,,,index_3,,
		// 如果处于等待的 index 区间 >= 刚刚更新的结束区间 => 有可能被通知到;
		// 如果处于等待的 index 区间 < 刚刚更新的结束区间 => 一定能被通知到;
		if uint64(len(indexWaiters)) >= curIndex-doneIndex {

			for i := doneIndex + 1; i <= curIndex; i++ {
				if waiters, ok := indexWaiters[i]; ok {
					notifyAndRemove(i, waiters)
				}
			}
		} else {
			for idx, waiters := range indexWaiters {
				if idx <= doneIndex {
					notifyAndRemove(index, waiters)
				}
			}
		}
	}

	for {
		select {
		case <-closer.CloseSignal:
			return
		case m := <-lm.markCh:
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
				// Begin(index) Done(index) 调用;
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
