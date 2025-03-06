package src

import (
	"github.com/pkg/errors"
	"sync"
	"trainKv/common"
	"trainKv/lsm"
	"trainKv/model"
	"trainKv/utils"
)

type TrainKVDB struct {
	Mux            sync.Mutex
	Lsm            *lsm.LSM
	vlog           *ValueLog
	Opt            *lsm.Options
	writeCh        chan *Request
	blockWrites    int32
	VlogReplayHead model.ValuePtr
	logRotates     int32
	Closer         *utils.Closer
}

func Open(opt *lsm.Options) (*TrainKVDB, error) {
	db := &TrainKVDB{Opt: opt}
	db.initVlog()
	db.Lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:             opt.WorkDir,
		MemTableSize:        opt.MemTableSize,
		SSTableMaxSz:        opt.SSTableMaxSz,
		BlockSize:           opt.BlockSize,
		BloomFalsePositive:  opt.BloomFalsePositive,
		CacheNums:           opt.CacheNums,
		NumCompactors:       2,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		TableSizeMultiplier: 2,
		BaseTableSize:       5 << 20,
		NumLevelZeroTables:  15,
		MaxLevelNum:         common.MaxLevelNum,
		DiscardStatsCh:      &(db.vlog.VLogFileDisCardStaInfo.FlushCh),
	})

	// .vlog 遍历maxID文件, 仅仅是获得,可写offset;
	if err := db.vlog.Open(func(e *model.Entry, vp *model.ValuePtr) error {
		return nil
	}); err != nil {
		common.Panic(err)
	}
	// 启动 sstable 的合并压缩过程
	//go db.Lsm.StartCompacter()
	db.writeCh = make(chan *Request)
	// 接收 vlog GC 重新写大量entry的请求
	go db.handleWriteCh()
	return db, nil
}

func (db *TrainKVDB) Get(key []byte) (model.Entry, error) {
	if key == nil {
		return model.Entry{}, common.ErrEmptyKey
	}
	internalKey := model.KeyWithTs(key)
	var (
		entry model.Entry
		err   error
	)
	if entry, err = db.Lsm.Get(internalKey); err != nil {
		return model.Entry{}, err
	}

	if lsm.IsDeletedOrExpired(entry) {
		return model.Entry{}, common.ErrKeyNotFound
	}

	if entry.Value != nil && model.IsValPtr(entry) {
		var vp model.ValuePtr
		vp.Decode(entry.Value)
		read, callBack, err := db.vlog.Read(&vp)
		defer model.RunCallback(callBack)
		if err != nil {
			return model.Entry{}, err
		}
		entry.Value = model.SafeCopy(nil, read)
	}
	entry.Key = key
	return entry, nil
}

func (db *TrainKVDB) Set(entry model.Entry) error {
	if entry.Key == nil || len(entry.Key) == 0 {
		return common.ErrEmptyKey
	}
	var (
		vp  *model.ValuePtr
		err error
	)
	entry.Key = model.KeyWithTs(entry.Key)
	entry.Version = model.ParseTsVersion(entry.Key)
	if !db.ShouldWriteValueToLSM(entry) {
		if vp, err = db.vlog.NewValuePtr(&entry); err != nil {
			return err
		}
		entry.Meta |= common.BitValuePointer
		entry.Value = vp.Encode()
	}
	return db.Lsm.Put(entry)
}

func (db *TrainKVDB) Del(key []byte, id uint64) error {
	return db.Set(model.Entry{
		Key:       key,
		Value:     nil,
		Meta:      common.BitDelete,
		ExpiresAt: 0,
	})
}

func (db *TrainKVDB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return nil
	}
	// Pick a log file and run GC. 寻找合适的 vlog 文件 进行 gc;
	return db.vlog.runGC(discardRatio)
}

// BatchSet batch set entries
// 1. vlog GC组件调用, 目的是加速 有效key重新写;
func (db *TrainKVDB) BatchSet(entries []*model.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	request, err := db.SendToWriteCh(entries)
	if err != nil {
		return err
	}
	// 同步等待
	return request.Wait()
}

// SendToWriteCh 发送数据到 db.writeCh 通道中; vlog 组件调用;
// 1. Re重放: openVlog() -> go vlog.flushDiscardStats(); 监听并收集vlog文件的GC信息, 必要时将序列化统计表数据, 发送到 db 的写通道中, 以便重启时可以直接获得;
// 2. GC重写: db.batchSet(); 批处理(加速vlog GC重写速度), 将多个 []entry 写到指定通道中;
func (db *TrainKVDB) SendToWriteCh(entries []*model.Entry) (*Request, error) {
	var count, size int64
	for _, entry := range entries {
		size += int64(entry.EstimateSize(int(db.Opt.ValueThreshold)))
		count++
	}
	if count > db.Opt.MaxBatchCount || size > db.Opt.MaxBatchSize {
		return nil, common.ErrBatchTooLarge
	}
	request := RequestPool.Get().(*Request)
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
	if err := db.vlog.Write(reqs); err != nil {
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
		if db.ShouldWriteValueToLSM(*entry) {
			entry.Meta &= ^common.BitValuePointer
		} else {
			entry.Meta |= common.BitValuePointer
			entry.Value = req.ValPtr[i].Encode()
		}
		if err := db.Lsm.Put(*entry); err != nil {
			return err
		}
	}
	return nil
}

func (db *TrainKVDB) ShouldWriteValueToLSM(entry model.Entry) bool {
	return len(entry.Value) < db.Opt.ValueThreshold
}

func (db *TrainKVDB) initVlog() {
	vlog := &ValueLog{
		DirPath:    db.Opt.WorkDir,
		Mux:        sync.RWMutex{},
		FilesToDel: make([]uint32, 0),
		VLogFileDisCardStaInfo: &VLogFileDisCardStaInfo{
			FileMap: make(map[uint32]int64),
			FlushCh: make(chan map[uint32]int64, 16),
			Closer:  utils.NewCloser(),
		},
	}
	vlog.Db = db
	vlog.Opt = db.Opt
	vlog.GarbageCh = make(chan struct{}, 1)
	db.vlog = vlog
}

func (db *TrainKVDB) Close() error {
	db.vlog.VLogFileDisCardStaInfo.Closer.Close()
	if err := db.Lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.Close(); err != nil {
		return err
	}
	return nil
}
