package model

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

var RequestPool = sync.Pool{
	New: func() interface{} {
		return new(Request)
	},
}

type Request struct {
	Entries []*Entry
	ValPtr  []ValuePtr // 值切片: 避免每条目取地址逃逸分配; LSM 直写条目以零值占位;
	Wg      sync.WaitGroup
	Err     error
	ref     int32

	valPtrScratch []byte // 12B 复用缓冲, 供 writeToLSM 编码 ValuePtr, 每条目同步消费后可覆写;
}

// EncodeValPtr 把 ValuePtr 的 12 字节写入请求级复用缓冲并返回;
// 该缓冲在 WriteRequest 处理完本请求前保持有效, 调用方须在 lsm.Put 同步消费;
func (r *Request) EncodeValPtr(p ValuePtr) []byte {
	if cap(r.valPtrScratch) < int(vptrSize) {
		r.valPtrScratch = make([]byte, vptrSize)
	}
	r.valPtrScratch = r.valPtrScratch[:vptrSize]
	*(*ValuePtr)(unsafe.Pointer(&r.valPtrScratch[0])) = p
	return r.valPtrScratch
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
