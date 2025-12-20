package model

import (
	"sync"
	"sync/atomic"
)

var RequestPool = sync.Pool{
	New: func() interface{} {
		return new(Request)
	},
}

type Request struct {
	Entries []*Entry
	ValPtr  []*ValuePtr
	Wg      sync.WaitGroup
	Err     error
	ref     int32
}

func (r *Request) IncrRef() {
	atomic.AddInt32(&r.ref, 1)
}

func (r *Request) DecrRef() {
	n := atomic.AddInt32(&r.ref, -1)
	if n > 0 {
		return
	}
	r.Entries = nil
	RequestPool.Put(r)
}

func (r *Request) Wait() error {
	r.Wg.Wait()
	err := r.Err
	r.DecrRef()
	return err
}

func (r *Request) Reset() {
	r.Entries = r.Entries[:0]
	r.ValPtr = r.ValPtr[:0]
	r.Wg = sync.WaitGroup{}
	r.Err = nil
	atomic.StoreInt32(&r.ref, 0)
}
