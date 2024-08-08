package lsm

import (
	"github.com/pkg/errors"
	"math"
	"sync"
	"trainKv/common"
	"trainKv/model"
)

type DBOptions struct {
	ValueThreshold      int64
	WorkDir             string
	MemTableSize        int64
	SSTableSize         int64
	MaxBatchCount       int64
	MaxBatchSize        int64
	ValueLogFileSize    int
	VerifyValueChecksum bool
	ValueLogMaxEntries  uint32
	LogRotatesToFlush   int32
	MaxTableSize        int64
}

type TrainKVDB struct {
	mux            sync.Mutex
	lsm            *LSM
	vlog           *ValueLog
	opt            *DBOptions
	writeCh        chan *Request
	blockWrites    int32
	vlogReplayHead *model.ValuePtr
	logRotates     int32
}

func Open(opt *DBOptions) (*TrainKVDB, error) {
	db := &TrainKVDB{opt: opt}
	db.initVLog()
	db.lsm = NewLSM(&Options{
		WorkDir:             opt.WorkDir,
		MemTableSize:        opt.MemTableSize,
		SSTableMaxSz:        opt.SSTableSize,
		BlockSize:           8 * 1024,
		BloomFalsePositive:  0,
		NumCompactors:       1,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		TableSizeMultiplier: 2,
		BaseTableSize:       5 << 20,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		DiscardStatsCh:      nil,
	})
	go db.lsm.StartCompacter()
	db.writeCh = make(chan *Request)
	go db.handleWriteCh()
	return db, nil
}
func (db *TrainKVDB) Get(key []byte) (*model.Entry, error) {
	if key == nil {
		return nil, common.ErrEmptyKey
	}
	internalKey := model.KeyWithTs(key, math.MaxUint32)
	var (
		entry *model.Entry
		err   error
	)
	if entry, err = db.lsm.Get(internalKey); err != nil {
		return entry, err
	}
	if entry != nil && IsDeletedOrExpired(entry) {
		return nil, common.ErrKeyNotFound
	}
	if entry != nil && model.IsValPtr(entry) {
		var vp *model.ValuePtr
		vp.Decode(entry.Value)
		read, callBack, err := db.vlog.read(vp)
		defer model.RunCallback(callBack)
		if err != nil {
			return nil, err
		}
		entry.Value = model.SafeCopy(nil, read)
	}
	entry.Key = key
	return entry, nil
}

func (db *TrainKVDB) Set(entry *model.Entry) error {
	if entry == nil || len(entry.Key) == 0 {
		return common.ErrEmptyKey
	}
	var (
		vp  *model.ValuePtr
		err error
	)
	entry.Key = model.KeyWithTs(entry.Key, math.MaxUint32)
	if !db.shouldWriteValueToLSM(entry) {
		if vp, err = db.vlog.NewValuePtr(entry); err != nil {
			return err
		}
		entry.Meta |= common.BitValuePointer
		entry.Value = vp.Encode()
	}
	return db.lsm.Put(entry)
}

func (db *TrainKVDB) Del(key []byte) error {
	return db.lsm.Put(&model.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})
}

func (db *TrainKVDB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return nil
	}
	return nil
}

func (db *TrainKVDB) initVLog() {

}

// BatchSet batch set entries
// 1. vlog GC组件调用, 目的是加速 有效key重新写;
func (db *TrainKVDB) BatchSet(entries []*model.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	request, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	// 同步等待
	return request.Wait()
}

// sendToWriteCh 发送数据到 db.writeCh 通道中; vlog 组件调用;
// 1. Re重放: go vlog.flushDiscardStats(); 将序列化统计表数据, 传送到 db 的写通道中, 以便重启时可以直接获得;
// 2. GC重写: db.batchSet(); 批处理(加速vlog重写速度), 将多个 []entry 写到指定通道中;
func (db *TrainKVDB) sendToWriteCh(entries []*model.Entry) (*Request, error) {
	var count, size int64
	for _, entry := range entries {
		size += int64(entry.EstimateSize(int(db.opt.ValueThreshold)))
		count++
	}
	if count > db.opt.MaxBatchCount || size > db.opt.MaxBatchSize {
		return nil, common.ErrBatchTooLarge
	}
	request := requestPool.Get().(*Request)
	request.Reset()
	request.Entries = entries
	request.Wg.Add(1)
	request.IncrRef()
	db.writeCh <- request
	return request, nil
}

func (db *TrainKVDB) handleWriteCh() {
	var reqLen int64
	reqs := make([]*Request, 0)
	for {
		var r *Request
		select {
		case r = <-db.writeCh:
			reqs = append(reqs, r)
			reqLen += int64(len(reqs))
			if reqLen >= 3*common.KVWriteChCapacity {
				go db.writeRequest(reqs)
				reqs = reqs[:0]
				reqLen = 0
			}
		}
	}
}

// writeRequests is called serially by only one goroutine.
// 1.各个vlog文件的失效数据统计表
// 2.vlog GC重写的entry[]
// 写完 vlog 后, 再逐一写到 lsm 中
func (db *TrainKVDB) writeRequest(reqs []*Request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, req := range reqs {
			req.Err = err
			req.Wg.Done()
		}
	}
	if err := db.vlog.write(reqs); err != nil {
		done(err)
		return err
	}
	var count int
	for _, req := range reqs {
		if len(req.Entries) == 0 {
			continue
		}
		count += len(req.Entries)
		if err := db.writeToLSM(req); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
	}
	done(nil)
	return nil
}

func (db *TrainKVDB) writeToLSM(req *Request) error {
	if len(req.ValPtr) != len(req.Entries) {
		return errors.Errorf("Ptrs and Entries don't match: %+v", req)
	}
	for i, entry := range req.Entries {
		if db.shouldWriteValueToLSM(entry) {
			entry.Meta &= ^common.BitValuePointer
		} else {
			entry.Meta |= common.BitValuePointer
			entry.Value = req.ValPtr[i].Encode()
		}
		if err := db.lsm.Put(entry); err != nil {
			return err
		}
	}
	return nil
}

func (db *TrainKVDB) shouldWriteValueToLSM(entry *model.Entry) bool {
	return int64(len(entry.Value)) < db.opt.ValueThreshold
}

func (db *TrainKVDB) Close() error {
	if err := db.lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.Close(); err != nil {
		return err
	}
	return nil
}
