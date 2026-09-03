package lsm

import (
	"sync"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/skl"
	"github.com/kebukeYi/TrainKV/v2/utils"
)

type LSM struct {
	sync.RWMutex
	memoryTable    *MemoryTable
	immemoryTables []*MemoryTable
	LevelManger    *LevelsManger
	Option         *Options

	flushMemTable chan *MemoryTable

	ExpiredValPtrChan chan model.ValuePtr // compact`MergeIterator`fix() to lsm;
	ExpiredValNum     int
	ExpiredValSize    int64
}

func NewLSM(opt *Options, closer *utils.Closer) *LSM {
	lsm := &LSM{
		Option:        opt,
		flushMemTable: make(chan *MemoryTable, opt.WaitFlushMemTables),
	}
	// 1. 更新 lm.maxID
	lsm.LevelManger = lsm.InitLevelManger(opt)

	go lsm.StartFlushMemTable(closer) // lock

	// 2. 更新 lm.maxID
	lsm.memoryTable, lsm.immemoryTables = lsm.recovery()
	for _, im := range lsm.immemoryTables {
		lsm.flushMemTable <- im
	}
	return lsm
}

func (lsm *LSM) Put(entry *model.Entry) (err error) {
	if entry.Key == nil || len(entry.Key) == 0 || len(entry.Key) <= 8 {
		return common.ErrEmptyKey
	}

	// 固定容量 arena: 以"已用字节 + 本条条目上界估算"是否超过容量来决定轮转;
	// 保证写入后 arena 绝不越界 (旧逻辑按 WAL 大小轮转, 无法约束 arena 用量);
	if lsm.memoryTable.MemBytes()+skl.EstimateEntryMemSize(entry) > lsm.Option.MemTableSize {
		lsm.Rotate()
	}

	// 1. 添加进跳表中的 key 是携带有 版本号;
	// 2. wal文件持久化的的 key 是携带有 版本号;
	err = lsm.memoryTable.Put(entry)
	if err != nil {
		return err
	}

	return err
}

// RotateIfNeed 请求级轮转预判: 同一请求 = 同一事务的全部数据条目 + finTxn 结束标记,
// 必须整体落在同一个 memtable/WAL 内; 逐条 Put 的轮转检查可能把 finTxn 拆进新 WAL
// (数据留在旧 WAL, 旧 WAL flush 后即删除), 重开重放时新 WAL 头部出现无主 FIN →
// recovery ErrBadTxn, 库无法打开; 调用方须在写入一个请求的所有条目之前调用本方法;
func (lsm *LSM) RotateIfNeed(entries []*model.Entry) {
	if len(entries) == 0 {
		return
	}
	var need int64
	for _, entry := range entries {
		need += skl.EstimateEntryMemSize(entry)
	}
	// 整请求超过单表容量(配置失衡)时, 一次轮转也无法保证同表, 退回逐条轮转的旧行为;
	if need > lsm.Option.MemTableSize {
		return
	}
	if lsm.memoryTable.MemBytes()+need > lsm.Option.MemTableSize {
		lsm.Rotate()
	}
}

func (lsm *LSM) SyncWalFile() error {
	if err := lsm.memoryTable.SyncWalFile(); err != nil {
		return err
	}
	return nil
}

func (lsm *LSM) Get(keyTs []byte) (model.Entry, error) {
	if len(keyTs) <= 8 {
		return model.Entry{Version: 0}, common.ErrEmptyKey
	}
	startTs := model.ParseTsVersion(keyTs)

	// Read of IncrRef; 无 immutable 时直接在栈上持有引用, 避免每次 Get 分配切片;
	lsm.RLock()
	mt := lsm.memoryTable
	mt.IncrRef()
	var imms []*MemoryTable
	if n := len(lsm.immemoryTables); n > 0 {
		imms = make([]*MemoryTable, n)
		for i := 0; i < n; i++ {
			im := lsm.immemoryTables[n-1-i] // 新→旧, 与 getAllMemoryTales 顺序一致;
			im.IncrRef()
			imms[i] = im
		}
	}
	lsm.RUnlock()
	defer func() { // DecrRef;
		mt.DecrRef()
		for _, im := range imms {
			im.DecrRef()
		}
	}()

	// 内存中的跳表去查询 ;
	entry, _ := mt.Get(keyTs)
	if entry.Version != 0 || entry.Value != nil {
		// 1. 跳表中对返回的near节点进行对比时, key 是去掉Ts时间戳的, 相同直接返回,将不再继续向level层寻找;
		// 否则向level--层寻找;
		if entry.Version == startTs {
			return entry, nil
		}
		// 2. 仅普通事务条目(带 BitTxn)可安全提前返回: 其 commitTs 单调递增且只写入最新 memtable,
		//    保证是本 key 的全局最新可见版本;
		//    vlogGC 重写条目(无 BitTxn, 见 vlog.go#gcReWriteLog)会把仍被 LSM 引用的旧版本复活进
		//    最新 memtable, 破坏"存储层新旧顺序 = 版本顺序"不变量, 必须走全层取最大兜底;
		if entry.Version <= startTs && (entry.Meta&common.BitTxn != 0) {
			return entry, nil
		}
	}

	for _, memoryTable := range imms {
		entry, _ = memoryTable.Get(keyTs)
		if entry.Version == 0 && entry.Value == nil {
			continue
		}
		if entry.Version == startTs {
			return entry, nil
		}
		// 同 mt: 仅普通事务条目可提前返回 (理由同上);
		if entry.Version <= startTs && (entry.Meta&common.BitTxn != 0) {
			return entry, nil
		}

	} // imms[] over

	// 2. level 0-7 层 进行寻找;
	return lsm.LevelManger.Get(keyTs)
}

func (lsm *LSM) MaxVersion() uint64 {
	lsm.RLock()
	var maxVersion uint64
	maxVersion = lsm.memoryTable.maxVersion
	for _, table := range lsm.immemoryTables {
		if table.maxVersion > maxVersion {
			maxVersion = table.maxVersion
		}
	}
	lsm.RUnlock()

	// 每层 tables 被 compact/flush 协程并发增删, 遍历须持层锁;
	for i := 0; i < common.MaxLevelNum; i++ {
		leh := lsm.LevelManger.levelHandlers[i]
		leh.mux.RLock()
		for _, table := range leh.tables {
			if v := table.MaxVersion(); v > maxVersion {
				maxVersion = v
			}
		}
		leh.mux.RUnlock()
	}

	return maxVersion
}

func (lsm *LSM) GetSkipListFromMemTable() *skl.SkipList {
	return lsm.memoryTable.skipList
}

func (lsm *LSM) Rotate() {
	lsm.Lock()
	im := lsm.memoryTable
	if lsm.memoryTable.skipList.Empty() {
		lsm.Unlock()
		return
	}
	lsm.immemoryTables = append(lsm.immemoryTables, lsm.memoryTable)
	lsm.memoryTable = lsm.NewMemoryTable()
	lsm.Unlock()

	// SyncWrites 下, 当前批次跨轮转的条目可能已写入旧表 WAL;
	// 在交予 flush 协程之前先同步旧 WAL, 兑现"提交返回 = 已持久化"的承诺:
	// flush 是异步的, SST 落盘发生在提交返回之后, 不能依赖它;
	if lsm.Option.SyncWrites {
		if err := im.wal.SyncFile(); err != nil {
			// SyncWrites 语义下, 已确认的提交可能丢失, 视为致命错误;
			panic(err)
		}
	}
	// 通道有可能阻塞;
	lsm.flushMemTable <- im
}

func (lsm *LSM) StartFlushMemTable(closer *utils.Closer) {
	defer closer.Done()
	flushIMemoryTable := func(im *MemoryTable) {
		// 空表直接跳过 (避免 0 字节 SST 构建), 有数据的表才 flush;
		if im == nil || im.Size() == 0 {
			return
		}
		if err := lsm.LevelManger.flush(im); err != nil {
			common.Panic(err)
		}
		lsm.Lock()
		lsm.immemoryTables = lsm.immemoryTables[1:]
		im.skipList.DecrRef()
		lsm.Unlock()
	}

	for {
		select {
		case im := <-lsm.flushMemTable:
			flushIMemoryTable(im)
		case <-closer.CloseSignal:
			for im := range lsm.flushMemTable {
				flushIMemoryTable(im)
			}
			return
		}
	}
}

func (lsm *LSM) CloseFlushIMemChan() {
	close(lsm.flushMemTable)
}

func (lsm *LSM) StartCompacter(closer *utils.Closer) {
	n := lsm.Option.NumCompactors
	for coroutineID := 0; coroutineID < n; coroutineID++ {
		go lsm.LevelManger.runCompacter(coroutineID, closer)
	}
}

func (lsm *LSM) Close() error {
	if lsm.memoryTable != nil {
		if err := lsm.memoryTable.close(false); err != nil {
			return err
		}
	}
	for i := range lsm.immemoryTables {
		if err := lsm.immemoryTables[i].close(false); err != nil {
			return err
		}
	}
	if err := lsm.LevelManger.close(); err != nil {
		return err
	}
	return nil
}
