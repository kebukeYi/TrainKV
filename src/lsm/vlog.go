package lsm

import (
	"sync"
	"sync/atomic"
	"trainKv/db"
	"trainKv/model"
	"trainKv/utils"
)

const discardStatsFlushThreshold = 100

var vlogFileDiscardStatsKey = []byte("corekv_discard")

type ValueLog struct {
	db                 *db.TrainKVDB
	dirPath            string
	mux                sync.RWMutex
	filesMap           map[uint32]*utils.VLogFile
	maxFid             uint32
	filesToDel         []uint32
	activeIteratorNum  int32
	writableFileOffset uint32
	entriesWrittenNum  uint32
	opt                *db.Options

	garbageCh              chan struct{}
	vLogFileDisCardStaInfo *VLogFileDisCardStaInfo
}

type VLogFileDisCardStaInfo struct {
	mux               sync.RWMutex
	file              map[uint32]int64
	flushCh           chan map[uint32]int64
	updatesSinceFlush int // flush 次数
}

func (l *ValueLog) Close() error {
	return nil
}

func (l *ValueLog) NewValuePtr(entry *model.Entry) (*model.ValuePtr, error) {
	return nil, nil
}

func (l *ValueLog) Read(vp *model.ValuePtr) ([]byte, func(), error) {
	return nil, nil, nil
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(Request)
	},
}

type Request struct {
	Entries []*model.Entry
	ValPtr  []*model.ValuePtr
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
	requestPool.Put(r)
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
	r.ref = 0
}
