package TrainKV

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/utils"
	"github.com/pkg/errors"
	"sync"
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
	Closer         closer
}

type closer struct {
	memtable   *utils.Closer
	compactors *utils.Closer
	writes     *utils.Closer
	valueGC    *utils.Closer
}

func Open(opt *lsm.Options) (*TrainKVDB, error, func() error) {
	if opt == nil {
		opt = lsm.GetLSMDefaultOpt("")
	}
	callBack, _ := lsm.CheckLSMOpt(opt)
	db := &TrainKVDB{Opt: opt}

	db.initVlog()
	opt.DiscardStatsCh = &db.vlog.VLogFileDisCardStaInfo.FlushCh

	db.Closer.memtable = utils.NewCloser(1)
	db.Lsm = lsm.NewLSM(opt, db.Closer.memtable)

	db.Closer.valueGC = utils.NewCloser(1)
	go db.vlog.waitOnGC(db.Closer.valueGC)

	// 3. 更新 lm.maxID
	if err := db.vlog.Open(func(e *model.Entry, vp *model.ValuePtr) error {
		return nil
	}); err != nil {
		common.Panic(err)
	}

	db.Closer.compactors = utils.NewCloser(1)
	go db.Lsm.StartCompacter(db.Closer.compactors)

	// 1.接收 vlog GC 重写大量entry的写请求;
	// 2.接收db.set(entry)的请求,使用通道的话,就不用加锁执行vlog.write();
	db.writeCh = make(chan *Request, lsm.KvWriteChCapacity)
	db.Closer.writes = utils.NewCloser(1)
	go db.handleWriteCh(db.Closer.writes)

	return db, nil, callBack
}

func (db *TrainKVDB) Get(key []byte) (model.Entry, error) {
	if key == nil || len(key) == 0 {
		return model.Entry{Version: -1}, common.ErrEmptyKey
	}
	internalKey := model.KeyWithTs(key)
	var (
		entry model.Entry
		err   error
	)
	if entry, err = db.Lsm.Get(internalKey); err != nil {
		return model.Entry{Version: -1}, err
	}
	// 1. 返回正常key-val
	// 2. 返回非正常key-val
	// 3. 返回大于key-val
	// 4. 返回小于key-val
	if lsm.IsDeletedOrExpired(entry) {
		return model.Entry{Version: -1}, common.ErrKeyNotFound
	}

	if entry.Value != nil && model.IsValPtr(entry) {
		var vp model.ValuePtr
		vp.Decode(entry.Value)
		read, callBack, err := db.vlog.Read(&vp)
		defer model.RunCallback(callBack)
		if err != nil {
			return model.Entry{Version: -1}, err
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
	entry.Key = model.KeyWithTestTs(entry.Key, uint64(entry.Version))
	//entry.Key = model.KeyWithTs(entry.Key)
	entry.Version = model.ParseTsVersion(entry.Key)
	err := db.BatchSet([]*model.Entry{&entry})
	return err
}

func (db *TrainKVDB) Del(key []byte, version int64) error {
	return db.Set(model.Entry{
		Key:       key,
		Version:   version,
		Value:     nil,
		Meta:      common.BitDelete,
		ExpiresAt: 0,
	})
}

func (db *TrainKVDB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return nil
	}
	// 寻找合适的 vlog 文件 进行 gc;
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
	return request.Wait()
}

// SendToWriteCh 发送数据到 db.writeCh 通道中; vlog 组件调用;
// 1. Re重放: openVlog() -> go vlog.flushDiscardStats(); 监听并收集vlog文件的GC信息, 必要时将序列化统计表数据, 发送到 db 的写通道中, 以便重启时可以直接获得;
// 2. GC重写: db.batchSet(); 批处理(加速vlog GC重写速度), 将多个 []entry 写到指定通道中;
func (db *TrainKVDB) SendToWriteCh(entries []*model.Entry) (*Request, error) {
	var count, size int64
	for _, entry := range entries {
		size += int64(entry.EstimateSize(db.Opt.ValueThreshold))
		count++
	}
	if count >= db.Opt.MaxBatchCount || size >= db.Opt.MaxBatchSize {
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

func (db *TrainKVDB) handleWriteCh(closer *utils.Closer) {
	defer closer.Done()
	var reqLen int64
	reqs := make([]*Request, 0, 10)
	for {
		var r *Request
		select {
		case <-closer.CloseSignal:
			for {
				select {
				case r = <-db.writeCh:
					reqs = append(reqs, r)
				default: // b.writeCh 中没有更多数据, 执行 default 分支;
					db.writeRequest(reqs)
					return
				}
			}
		case r = <-db.writeCh:
			reqs = append(reqs, r)
			reqLen = int64(len(reqs))
			if reqLen >= 3*common.KVWriteChRequestCapacity {
				go db.writeRequest(reqs)
				reqs = make([]*Request, 0, 1)
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
			entry.Meta = 0
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
		},
	}
	vlog.Db = db
	vlog.Opt = db.Opt
	db.vlog = vlog
	vlog.GarbageCh = make(chan struct{}, 1) //一次只允许运行一个vlogGC协程
}

func (db *TrainKVDB) Close() error {
	db.Closer.valueGC.CloseAndWait()
	db.Closer.writes.CloseAndWait()
	close(db.writeCh)
	db.Lsm.CloseFlushIMemChan()
	db.Closer.memtable.CloseAndWait()
	db.Closer.compactors.CloseAndWait()

	if err := db.Lsm.Close(); err != nil {
		return err
	}

	if err := db.vlog.Close(); err != nil {
		return err
	}
	fmt.Println("db.Close exit.")
	return nil
}
