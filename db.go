package TrainKV

import (
	"bytes"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/gofrs/flock"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/utils"
	"github.com/pkg/errors"
)

type TrainKV struct {
	Lsm                *lsm.LSM
	vlog               *ValueLog
	fileLock           *flock.Flock
	Opt                *lsm.Options
	transactionManager *TransactionManager
	txnPool            sync.Pool
	writeCh            chan *model.Request
	VlogReplayHead     model.ValuePtr
	logRotates         int32
	Closer             closer
	closeOnce          sync.Once
	isClosed           atomic.Bool
}

type closer struct {
	memtable   *utils.Closer
	compactors *utils.Closer
	writes     *utils.Closer
	valueGC    *utils.Closer
}

func Open(opt *lsm.Options) (*TrainKV, error, func() error) {
	if opt == nil {
		opt = lsm.GetDefaultOpt("")
	}
	callBack, _ := lsm.CheckOpt(opt)
	db := &TrainKV{Opt: opt}
	db.txnPool.New = func() interface{} {
		return &Transaction{}
	}
	join := filepath.Join(opt.WorkDir, common.LockFile)
	fileLock := flock.New(join)
	db.fileLock = fileLock
	hold, err := fileLock.TryLock()
	if err != nil || !hold {
		return nil, common.ErrLockDB, callBack
	}
	db.isClosed.Store(false)
	db.initVlog()
	opt.DiscardStatsCh = &db.vlog.VLogFileDisCardStaInfo.FlushCh

	db.Opt.TxnDoneIndexCh = make(chan uint64, 1)
	db.Closer.memtable = utils.NewCloser(1)
	db.Lsm = lsm.NewLSM(opt, db.Closer.memtable)

	db.transactionManager = NewTransactionManager(opt)
	db.transactionManager.nextTxnTs = db.MaxVersion()
	db.transactionManager.startMark.Done(db.transactionManager.nextTxnTs)
	db.transactionManager.commitMark.Done(db.transactionManager.nextTxnTs)
	db.transactionManager.incrementNextTs()

	// 1.接收 vlog GC 重写大量entry[]的写请求;
	// 2.接收 txn.set(entry)的请求,使用通道的话,就不用加锁执行vlog.write();
	db.writeCh = make(chan *model.Request, lsm.KvWriteChCapacity)
	db.Closer.writes = utils.NewCloser(1)
	go db.handleWriteCh(db.Closer.writes)

	db.Closer.valueGC = utils.NewCloser(1)
	go db.vlog.waitOnGC(db.Closer.valueGC)

	// 3. 更新 lm.maxID
	if err := db.vlog.Open(func(e *model.Entry, vp *model.ValuePtr) error {
		return nil
	}); err != nil {
		common.Panic(err)
	}

	db.Closer.compactors = utils.NewCloser(db.Lsm.Option.NumCompactors)
	go db.Lsm.StartCompacter(db.Closer.compactors)

	return db, nil, callBack
}

func (db *TrainKV) GetTransactionManager() *TransactionManager {
	return db.transactionManager
}

func (db *TrainKV) get(keyMaxStartTs []byte) (*model.Entry, error) {
	if keyMaxStartTs == nil || len(keyMaxStartTs) == 0 {
		return nil, common.ErrEmptyKey
	}
	var (
		entry model.Entry
		err   error
	)

	if entry, err = db.Lsm.Get(keyMaxStartTs); err != nil {
		return nil, err
	}

	if lsm.IsDeletedOrExpired(&entry) {
		return nil, common.ErrKeyNotFound
	}

	if entry.Value != nil && model.IsValPtr(&entry) {
		var vp model.ValuePtr
		vp.Decode(entry.Value)
		read, callBack, err := db.vlog.Read(&vp)
		defer callBack()
		if err != nil {
			return nil, err
		}
		entry.Value = model.SafeCopy(nil, read)
	}
	entry.Key = model.ParseKey(keyMaxStartTs)
	return &entry, nil
}

func (db *TrainKV) MaxVersion() uint64 {
	return db.Lsm.MaxVersion()
}

func (db *TrainKV) set(entry *model.Entry) error {
	if entry.Key == nil || len(entry.Key) == 0 {
		return common.ErrEmptyKey
	}
	entry.Version = model.ParseTsVersion(entry.Key)
	err := db.BatchSet([]*model.Entry{entry})
	return err
}

func (db *TrainKV) del(key []byte) error {
	return db.set(&model.Entry{
		Key:       key,
		Value:     nil,
		Meta:      common.BitDelete,
		ExpiresAt: 0,
	})
}

func (db *TrainKV) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return nil
	}
	// 寻找合适的 vlog 文件 进行 gc;
	return db.vlog.runGC(discardRatio)
}

// BatchSet batch set entries
// 1. vlog GC组件调用, 目的是加速 有效key重新写;
func (db *TrainKV) BatchSet(entries []*model.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	request, err := db.SendToWriteCh(entries)
	if err != nil {
		return err
	}
	return request.Wait()
}

// SendToWriteCh 发送数据到 db.writeCh 通道中;
// 1. vlog.Re重放: openVlog() -> go vlog.flushDiscardStats(); 监听并收集vlog文件的GC信息, 必要时将序列化统计表数据, 发送到 db 的写通道中, 以便重启时可以直接获得;
// 2. vlog.GC重写: db.batchSet(); 批处理(加速vlog GC重写速度), 将多个 []entry 写到指定通道中;
// 3. db.set(): 串行化写流程,避免vlog和wal加锁;
func (db *TrainKV) SendToWriteCh(entries []*model.Entry) (*model.Request, error) {
	var count, size int64
	for _, entry := range entries {
		size += int64(entry.EstimateSize(db.Opt.ValueThreshold))
		count++
	}
	if count >= db.Opt.MaxBatchCount || size >= db.Opt.MaxBatchSize {
		return nil, common.ErrBatchTooLarge
	}
	request := model.RequestPool.Get().(*model.Request)
	request.Reset()
	request.Entries = entries
	request.Wg.Add(1)
	request.IncrRef()
	db.writeCh <- request
	return request, nil
}

func (db *TrainKV) handleWriteCh(closer *utils.Closer) {
	defer closer.Done()
	var reqLen int64
	reqs := make([]*model.Request, 0, 10)
	blockChan := make(chan struct{}, 1) //限制:每次只允许一个协程去写数据;

	writeRequest := func(reqs []*model.Request) {
		// WriteRequest 内部已通过 done(err) 把错误投递给批次中每个请求的提交方;
		// 这里不能 panic, 否则一次轮转等失败会直接杀死整个进程;
		common.Err(db.WriteRequest(reqs))
		<-blockChan
	}

	for {
		var r *model.Request
		select {
		case <-closer.CloseSignal:
			// 收到关闭信号, 迅速收集 req, 然后再发送;
			for {
				select {
				case r = <-db.writeCh:
					reqs = append(reqs, r)
				default: // db.writeCh 中没有更多数据, 执行 default 分支;
					blockChan <- struct{}{}
					writeRequest(reqs)
					return
				}
			}
		case r = <-db.writeCh:
			reqs = append(reqs, r)
			reqLen = int64(len(reqs))

			// 只要通道里还有请求就继续攒批, 攒满阈值或通道排空后, 立即把当前批次交给写协程;
			// 批次交出去之前绝不回到外层 select, 否则已攒批次会被搁置在 reqs 中,
			// 只有等到下一个请求到来才有机会被处理;
			collected := true
			// 在这for{}收集过程中, 突然宕机怎么办?
			for collected && reqLen < common.WriteChBatchThreshold {
				select {
				case r = <-db.writeCh:
					reqs = append(reqs, r)
					reqLen = int64(len(reqs))
				default: // db.writeCh 中没有更多数据, 执行 default 分支;
					collected = false
				}
			}

			// 令牌空闲时直接在本协程写盘: 串行提交场景下每批省去一个写协程的创建与调度;
			// 令牌被占用时(上一批仍在写), 才派发协程排队写, 本协程继续攒批;
			select {
			case blockChan <- struct{}{}:
				common.Err(db.WriteRequest(reqs))
				<-blockChan
				reqs = reqs[:0] // 复用批次切片底层数组;
			default:
				go writeRequest(reqs)
				reqs = make([]*model.Request, 0, 10)
			}
			reqLen = 0
		}
	} // for over
}

// WriteRequest is called serially by only one goroutine.
// 1.会收到 各个vlog文件的失效数据统计表;
// 2.会收到 vlog GC重写的entry[];
// 3.会收到 txn 提交的 entry[];
// 方法逻辑: 先写进 vlogFile, 后再写到 lsm 中;
func (db *TrainKV) WriteRequest(reqs []*model.Request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, req := range reqs {
			req.Err = err
			req.Wg.Done()
		}
	}

	wrote, err := db.vlog.Write(reqs)
	if err != nil {
		done(err)
		return err
	}

	// 先同步 vlog, 保证 WAL 中 ValuePtr 指向的 value 已持久化; 再写 WAL 并同步;
	if db.Lsm.Option.SyncWrites && wrote {
		if err = db.vlog.Sync(); err != nil {
			done(err)
			return errors.Wrap(err, "#WriteRequest.vlog.Sync()")
		}
	}

	for _, req := range reqs {
		if len(req.Entries) == 0 {
			continue
		}
		if err = db.writeToLSM(req); err != nil {
			done(err)
			return errors.Wrap(err, "#WriteRequest.writeToLSM()")
		}
	}

	if db.Lsm.Option.SyncWrites {
		// 每次批量写入后统一刷盘一次, 提交方会在 Wg.Done 之后才返回;
		if err = db.Lsm.SyncWalFile(); err != nil {
			done(err)
			return errors.Wrap(err, "#WriteRequest.SyncWalFile()")
		}
	}

	done(nil)
	return nil
}

func (db *TrainKV) writeToLSM(req *model.Request) error {
	if len(req.ValPtr) != len(req.Entries) {
		return errors.Errorf("#writeToLSM: Ptrs and Entries don't match: %+v", req)
	}
	for i, entry := range req.Entries {
		if db.ShouldWriteValueToLSM(entry) {
			// 以防万一;
			entry.Meta &= ^(common.BitValuePointer)
		} else {
			entry.Meta |= common.BitValuePointer
			// 写入请求级复用缓冲, 避免每条目分配 12B; lsm.Put 同步拷贝后即可覆写;
			entry.Value = req.EncodeValPtr(req.ValPtr[i])
		}
		// 确保写入的 entry 是安全的;
		if err := db.Lsm.Put(entry); err != nil {
			return err
		}
	}
	return nil
}

func (db *TrainKV) ShouldWriteValueToLSM(entry *model.Entry) bool {
	return int64(len(entry.Value)) < db.Opt.ValueThreshold
}

func (db *TrainKV) initVlog() {
	vlog := &ValueLog{
		DirPath:            db.Opt.WorkDir,
		filesLock:          sync.RWMutex{},
		FilesToDel:         make([]uint32, 0),
		Opt:                db.Opt,
		buf:                &bytes.Buffer{},
		runGCOver:          &sync.WaitGroup{},
		disCardStaInfoOver: &sync.WaitGroup{},
		VLogFileDisCardStaInfo: &VLogFileDisCardStaInfo{
			FileMap: make(map[uint32]int64),
			FlushCh: make(chan map[uint32]int64, 16),
			stopCh:  make(chan struct{}),
		},
	}
	vlog.Db = db
	db.vlog = vlog
	vlog.GarbageCh = make(chan struct{}, 1) //一次只允许运行一个vlogGC协程;
}

func (db *TrainKV) Close() error {
	if db.isClosed.Load() {
		return common.ErrClosedDB
	}
	db.isClosed.Store(true)

	// 1. 先停掉所有 writeCh 的生产者
	db.Closer.valueGC.CloseAndWait()    // vlog GC(BatchSet → writeCh)
	db.Closer.compactors.CloseAndWait() // 压缩(产生 discard stats → FlushCh)
	db.transactionManager.Stop()        // 事务提交(BatchSet → writeCh)
	db.vlog.stopDiscardStats()          // 统计 flush(排空 → writeCh)

	// 2. 再停消费者并排空 writeCh, 此刻无新生产者;
	db.Closer.writes.CloseAndWait()

	// 3. 最后刷 memtable / 关 LSM / 关 vlog 文件
	db.Lsm.CloseFlushIMemChan()
	db.Closer.memtable.CloseAndWait()

	if err := db.Lsm.Close(); err != nil {
		return err
	}

	if err := db.vlog.Close(); err != nil {
		panic(err)
	}

	return db.fileLock.Unlock()
}

func (db *TrainKV) IsClosed() bool {
	return db.isClosed.Load()
}

func BuildRequest(entries []*model.Entry) *model.Request {
	request := model.RequestPool.Get().(*model.Request)
	request.Reset()
	request.Entries = entries
	request.Wg.Add(1)
	request.IncrRef()
	return request
}

type TxnIterator struct {
	iter    interfaces.Iterator
	vlog    *ValueLog
	txn     *Transaction
	lastKey []byte // 新增:用于多版本去重
}

func (txn *Transaction) NewIterator(opt *interfaces.Options) *TxnIterator {
	if txn.discard {
		return nil
	}
	txn.db.vlog.incrIteratorCount()
	iters := make([]interfaces.Iterator, 0)
	iters = append(iters, txn.db.Lsm.NewLsmIterator(opt)...)

	// 把 pending 迭代器 也 接进合并集合:
	if pi := txn.newPendingWritesIterator(opt.IsAsc); pi != nil {
		iters = append(iters, pi)
	}

	res := &TxnIterator{
		iter: lsm.NewMergingIterator(iters, opt),
		vlog: txn.db.vlog,
		txn:  txn,
	}
	return res
}

func (txnIter *TxnIterator) Name() string {
	return "TxnIterator"
}
func (txnIter *TxnIterator) Next() {
	txnIter.iter.Next()
}
func (txnIter *TxnIterator) Seek(key []byte) {
	txnIter.iter.Seek(key)
}
func (txnIter *TxnIterator) Rewind() {
	txnIter.iter.Rewind()
}
func (txnIter *TxnIterator) Valid() bool {
	return txnIter.iter.Valid()
}
func (txnIter *TxnIterator) Item() interfaces.Item {
	var lazyVP model.ValuePtr
	var lazyVlog model.ValueReader
	for txnIter.iter.Valid() {
		lazyVlog = nil // 每条目重置惰性标记;
		entry := txnIter.iter.Item().Item
		// ① 可见性 + 删除标记过滤(5f99f3e 已加删除过滤,保留)
		if !txnIter.txn.IsVisible(&entry) {
			txnIter.iter.Next()
			continue
		} else {
			if (entry.Meta & common.BitDelete) > 0 {
				txnIter.iter.Next()
				continue
			}
		}

		// ② 大 value 惰性化: 只记录 ValuePtr 位置, Item.Value() 按需从 vlog 解码;
		//    扫描大 value 不再逐条拷贝 (如 1MB value 扫描直接省 1MB/条);
		if entry.Value != nil && model.IsValPtr(&entry) {
			lazyVP.Decode(entry.Value)
			lazyVlog = txnIter.vlog
			entry.Value = nil
		}

		// ③ 事务自己的 pending 写覆盖(读己之写 + 与 LSM 版本冲突时取 pending)
		raw := model.ParseKey(entry.Key)
		if txnIter.txn.update && txnIter.txn.pendingKeys != nil {
			if pe, ok := txnIter.txn.pendingKeys[utils.MemHash(raw)]; ok && bytes.Equal(pe.Key, raw) {
				entry.Key = model.KeyWithTs(pe.Key, txnIter.txn.startTs)
				entry.Version = txnIter.txn.startTs
			}
		}

		// ④ 多版本去重:同 key 只返回第一个(版本号越大越靠前 → 第一个=最新可见)
		if bytes.Equal(txnIter.lastKey, raw) {
			txnIter.iter.Next()
			continue
		}
		txnIter.lastKey = append(txnIter.lastKey[:0], raw...)
		if lazyVlog != nil {
			return interfaces.Item{Item: entry, VP: lazyVP, Vlog: lazyVlog}
		}
		return interfaces.Item{Item: entry}
	}
	return interfaces.Item{Item: model.Entry{Version: 0}}
}
func (txnIter *TxnIterator) Close() error {
	err := txnIter.vlog.decrIteratorCount()
	if err != nil {
		return err
	}
	return txnIter.iter.Close()
}
