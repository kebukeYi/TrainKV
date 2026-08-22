package TrainKV

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/utils"
)

type TransactionManager struct {
	detectConflicts bool
	tsLock          sync.Mutex
	writeChLock     sync.Mutex
	nextTxnTs       uint64
	startMark       *utils.LimitMark // startMark 回答"谁还在读" → 指导 compaction 安全回收旧版本;
	commitMark      *utils.LimitMark // commitMark 回答"谁提交完了" → 保证新事务开启时读到全部已提交数据;
	commitedTxns    []commitedTxn
	lastCleanupTs   uint64
	closer          *utils.Closer
}

type commitedTxn struct {
	TxnCommitTs  uint64
	conflictKeys []uint64 // 写 key 哈希快照; 提交时从 txn.conflictKeys 拷贝, 使 txn 的 map 可被池化复用;
}

func NewTransactionManager(options *lsm.Options) *TransactionManager {
	tm := &TransactionManager{
		detectConflicts: options.DetectConflicts,
		startMark:       &utils.LimitMark{Name: "startMark"},
		commitMark:      &utils.LimitMark{Name: "commitMark"},
		closer:          utils.NewCloser(2),
	}
	tm.startMark.Init(tm.closer, options.TxnDoneIndexCh)
	tm.commitMark.Init(tm.closer, nil)
	return tm
}

func (m *TransactionManager) Stop() {
	//需要等待 2次 done();
	m.closer.CloseAndWait()
}

func (m *TransactionManager) startTs(update bool) uint64 {
	m.tsLock.Lock()
	// 每次申请时, 申请到最新的index, 然后就等待其结束;
	// 只有最新的结束了, 才说明之前的都提交了, 类似串行;
	startTs := m.nextTxnTs - 1
	m.startMark.Begin(startTs)
	m.tsLock.Unlock()
	// 只有纯读事务会在这里等待;
	if !update {
		// 纯读事务在开启这一刻就向自己承诺:"所有编号 ≤ startTs 的提交, 都应该看得见";
		// 写事务的承诺, 延迟到首次读 LSM 时兑现(ensureStartTsReady), 纯写事务不付这笔等待;
		err := m.commitMark.WaitForIndexDone(context.Background(), startTs)
		common.Check(err)
	}
	return startTs
}

func (m *TransactionManager) incrementNextTs() {
	m.tsLock.Lock()
	m.nextTxnTs++
	m.tsLock.Unlock()
}

func (m *TransactionManager) DiscardTs() uint64 {
	return m.startMark.GetDoneIndex()
}
func (m *TransactionManager) hasConflict(txn *Transaction) bool {
	if !m.detectConflicts {
		return false
	}
	if len(txn.readKeys) == 0 {
		return false
	}
	for _, commit := range m.commitedTxns {
		if txn.startTs >= commit.TxnCommitTs {
			continue
		}
		// 找出当前事务开始之后提交的事务，判断自己读到的 key 中，是否存在于其他事务的写列表中;
		// txn.startTs < commit.TxnCommitTs
		for _, key := range txn.readKeys {
			for _, ck := range commit.conflictKeys {
				if ck == key {
					return true
				}
			}
		}
	}
	return false
}
func (m *TransactionManager) newCommitTs(txn *Transaction) (uint64, bool) {
	m.tsLock.Lock()
	defer m.tsLock.Unlock()
	if m.hasConflict(txn) {
		return 0, true
	}
	var commitTs uint64
	m.doneStart(txn)
	// 利用它的堆数据结构来跟踪当前活跃的事务的时间戳范围，用于找出哪些事务可以过期回收;
	m.cleanCommitedTransaction()
	commitTs = m.nextTxnTs
	m.nextTxnTs++
	m.commitMark.Begin(commitTs)
	utils.AssertTrue(commitTs >= m.lastCleanupTs)
	if m.detectConflicts {
		// 拷贝 当前事务的 写key[]哈希快照, 目的是 txn.conflictKeys[] 在事务结束后, 即被池化清除,减少引用,降低GC;
		keys := make([]uint64, 0, len(txn.conflictKeys))
		for k := range txn.conflictKeys {
			keys = append(keys, k)
		}
		m.commitedTxns = append(m.commitedTxns, commitedTxn{
			TxnCommitTs:  commitTs,
			conflictKeys: keys,
		})
	}
	return commitTs, false
}
func (m *TransactionManager) doneCommit(commitTs uint64) {
	m.commitMark.Done(commitTs)
}
func (m *TransactionManager) cleanCommitedTransaction() {
	if !m.detectConflicts {
		return
	}
	maxStartTs := m.startMark.GetDoneIndex()
	utils.AssertTrue(maxStartTs >= m.lastCleanupTs)
	if maxStartTs == m.lastCleanupTs {
		return
	}
	m.lastCleanupTs = maxStartTs
	tmp := m.commitedTxns[:0]
	for _, txn := range m.commitedTxns {
		// 如果历史事务的提交时间戳早于当前活跃的事务的开始时间戳，
		// 冲突检查时就不需要考虑它了，也就可以在 committedTxns 中回收;
		if txn.TxnCommitTs <= maxStartTs {
			continue
		} else {
			tmp = append(tmp, txn)
		}
	}
	m.commitedTxns = tmp
}
func (m *TransactionManager) doneStart(txn *Transaction) {
	if !txn.startDone {
		txn.startDone = true
		m.startMark.Done(txn.startTs)
	}
}

type Transaction struct {
	startTs      uint64
	readKeys     []uint64
	pendingKeys  map[uint64]*model.Entry
	conflictKeys map[uint64]struct{}
	count        int64
	size         int64
	db           *TrainKV
	commitTs     uint64
	numIterators atomic.Int32
	startDone    bool
	update       bool
	discard      bool
	startTsReady bool // 读事务开启即等待水位; 写事务延迟到首次读 LSM (ensureStartTsReady);

	// 池化复用的内部 scratch, 仅提交路径使用, 不参与事务语义;
	entries  []*model.Entry // 提交批次的 entry 切片, 复用底层数组;
	finEntry *model.Entry   // finTxn 结束标记 entry, 复用一个对象;
	finTsBuf []byte         // finTxn 标记 ts 的序列化缓冲, 复用底层数组;
	keyBufs  []*[]byte      // 提交路径从 KeyTsBufPool 取出的 key+8 缓冲指针, Discard 时归还;
}

// txnKeyBytes 事务标记键的静态字节; 只经只读路径进入 WAL, 不会被修改;
var txnKeyBytes = []byte(common.TxnKey)

func (db *TrainKV) NewTransaction(update bool) *Transaction {
	var txn *Transaction
	if v := db.txnPool.Get(); v != nil {
		txn = v.(*Transaction)
	} else {
		txn = &Transaction{}
	}
	// 注意: txnPool 取出时必须复位, Get 拿回来的对象带着上次事务的残留数据!!!
	txn.db = db
	txn.update = update
	txn.count = 1
	txn.size = int64(len(common.TxnKey) + 10)
	txn.commitTs = 0
	txn.discard = false
	txn.startDone = false
	// 只有纯读事务 才会等;
	txn.startTsReady = !update
	txn.readKeys = nil
	// 对空slice(nil) 切片操作合法; len=0 , cap>0;
	txn.keyBufs = txn.keyBufs[:0]
	txn.numIterators.Store(0)
	// 判断: 纯写事务 or 读写事务 or 纯读事务;
	if update {
		// 开启了冲突检测
		if db.Opt.DetectConflicts {
			// conflictKeys 在提交时, 快照复制 进 commitedTxns[], 事务结束后可池化复用;
			// 判断 == nil 只是判断是否是 新建 Transaction{}
			if txn.conflictKeys == nil {
				txn.conflictKeys = make(map[uint64]struct{})
			}
		}
		// pendingKeys 在 Discard 时 clear, 底层 池化复用;
		if txn.pendingKeys == nil {
			// 判断 == nil 只是判断是否是 新建 Transaction{}
			txn.pendingKeys = make(map[uint64]*model.Entry)
		}
	} else {
		// 纯读事务
		txn.pendingKeys = nil
		txn.conflictKeys = nil
	}
	txn.startTs = db.transactionManager.startTs(update)
	return txn
}

// ensureStartTsReady 兑现 "所有 ≤ startTs 的提交都可见" 的承诺;
// 写事务只有在真正读 LSM 时才需要, 纯写事务全程不付这笔等待,只需 lock() 等待 commitTs ;
func (t *Transaction) ensureStartTsReady() {
	if t.startTsReady {
		return
	}
	err := t.db.transactionManager.commitMark.WaitForIndexDone(context.Background(), t.startTs)
	common.Check(err)
	t.startTsReady = true
}

func (t *Transaction) IsVisible(e *model.Entry) bool {
	if e == nil {
		return false
	}
	tsVersion := model.ParseTsVersion(e.Key)
	return t.startTs >= tsVersion
}
func (t *Transaction) modify(e *model.Entry) error {
	switch {
	case !t.update:
		return common.ErrReadOnlyTxn
	case t.discard:
		return common.ErrDiscardedTxn
	case len(e.Key) == 0:
		return common.ErrEmptyKey
	case len(e.Key) > common.MaxKeySize:
		return exceedsSize("Key", common.MaxKeySize, e.Key)
	}

	if err := t.checkSize(e); err != nil {
		return err
	}

	var hash1, hash2 uint64
	if t.db.Opt.DetectConflicts {
		// 两个哈希都登记, 消除单 64 位哈希碰撞导致的误报冲突;
		hash1, hash2 = utils.KeyToHash(e.Key)
		t.conflictKeys[hash1] = struct{}{}
		t.conflictKeys[hash2] = struct{}{}
	} else {
		hash1 = utils.MemHash(e.Key)
	}
	e.Version = t.startTs
	// e.key is without ts;
	// pendingKeys 以 64 位哈希为键, 省去 string(e.Key) 的转换分配;
	// 读取时仍用 bytes.Equal 校验真实 key, 哈希碰撞只可能造成概率极低的写覆盖;
	// 同一事务内重复写同一 key(或哈希碰撞)会顶掉旧条目, 旧条目归还池中, 避免池化对象泄漏;
	if old, ok := t.pendingKeys[hash1]; ok {
		old.Release()
	}
	t.pendingKeys[hash1] = e
	return nil
}

func exceedsSize(prefix string, max int64, key []byte) error {
	return fmt.Errorf("%s with size %d exceeded %d limit. %s:\n%s",
		prefix, len(key), max, prefix, hex.Dump(key[:1<<10]))
}

func (t *Transaction) checkSize(e *model.Entry) error {
	count := t.count + 1
	size := t.size + int64(e.EstimateSize(t.db.Opt.ValueThreshold)+10)
	if count >= t.db.Opt.MaxBatchCount || size >= t.db.Opt.MaxBatchSize {
		return common.ErrBatchTooLarge
	}
	t.count = count
	t.size = size
	return nil
}
func (t *Transaction) Set(key, value []byte) error {
	entry := model.NewEntry(key, value)
	return t.modify(entry)
}
func (t *Transaction) SetEntry(entry *model.Entry) error {
	return t.modify(entry)
}
func (t *Transaction) Get(keyNoTs []byte) (*model.Entry, error) {
	if len(keyNoTs) == 0 {
		return nil, common.ErrEmptyKey
	} else if t.discard {
		return nil, common.ErrDiscardedTxn
	}
	if t.update {
		// key no version;
		if e, ok := t.pendingKeys[utils.MemHash(keyNoTs)]; ok && bytes.Equal(e.Key, keyNoTs) {
			if model.IsDeletedOrExpired(e.Meta, e.ExpiresAt) {
				return nil, common.ErrKeyNotFound
			}
			e.Version = t.startTs
			entry := e.SafeCopy()
			return &entry, nil
		}
		t.addReadKey(keyNoTs)
	}

	// 读 LSM 前先兑现 startTs 水位承诺 (读事务开启时已兑现, 这里是写事务的延迟兑现点);
	t.ensureStartTsReady()
	keyMaxStartTs := model.KeyWithTs(keyNoTs, t.startTs)
	entry, err := t.db.get(keyMaxStartTs)
	if err != nil {
		return nil, err
	}
	if entry.Value == nil && entry.Meta == 0 {
		return nil, common.ErrKeyNotFound
	}
	if model.IsDeletedOrExpired(entry.Meta, entry.ExpiresAt) {
		return nil, common.ErrKeyNotFound
	}
	return entry, nil
}

func (t *Transaction) Delete(key []byte) error {
	entry := model.NewEntry(key, nil)
	entry.Meta = common.BitDelete
	return t.modify(entry)
}

func (t *Transaction) addReadKey(key []byte) {
	if t.update {
		hash, _ := utils.KeyToHash(key)
		t.readKeys = append(t.readKeys, hash)
	}
}

func (t *Transaction) Commit() (uint64, error) {
	if t.discard {
		return 0, common.ErrDiscardedTxn
	}
	defer t.Discard()
	callBack, err := t.commitAndSendToDB()
	if err != nil {
		return 0, err
	}
	commitTs, err := callBack()
	if err != nil {
		return 0, err
	}
	return commitTs, nil
}

func (t *Transaction) commitAndSendToDB() (func() (uint64, error), error) {
	manager := t.db.transactionManager
	manager.writeChLock.Lock()
	defer manager.writeChLock.Unlock()
	commitTs, hasConflicts := manager.newCommitTs(t)
	if hasConflicts {
		return nil, common.ErrConflict
	}

	entries := t.entries[:0]
	t.keyBufs = t.keyBufs[:0]
	for _, entry := range t.pendingKeys {
		entry.Version = commitTs
		// len(key)+8 的整体空间, 池化;
		bufPtr := model.KeyWithTsPooled(entry.Key, commitTs)
		entry.Key = *bufPtr
		// 保存 指针*[len(key)+8]的整体空间, 目的是之后的归还;
		t.keyBufs = append(t.keyBufs, bufPtr)
		entry.Meta |= common.BitTxn
		entries = append(entries, entry)
	}

	// finTxn 结束标记 entry 及其 ts 序列化缓冲均为池化事务的 私有领域, 提交完成后, 不清理即可复用;
	entry := t.finEntry
	if entry == nil {
		entry = &model.Entry{}
		t.finEntry = entry
	}
	// 复用;
	t.finTsBuf = strconv.AppendUint(t.finTsBuf[:0], commitTs, 10)
	// 复用;
	entry.Key = txnKeyBytes
	entry.Value = t.finTsBuf
	entry.Version = commitTs
	entry.Meta = common.BitFinTxn
	entry.ExpiresAt = 0
	entry.HeaderLen = 0
	entry.Offset = 0
	entry.ValThreshold = 0
	entries = append(entries, entry)
	t.entries = entries

	req, err := t.db.SendToWriteCh(entries)
	if err != nil {
		// 整个数据写入失败, (不存在写入一半的错误);
		// 结束标记位, 并释放锁, 允许其他事务写入;
		manager.doneCommit(commitTs)
		return nil, err
	}
	// 写入通道成功; 释放锁, 允许其他事务写入或者读;
	ret := func() (uint64, error) {
		// 释放了锁后, 阻塞等待lsm结果; 然后才允许, 结束当前水印;
		err := req.Wait()
		manager.doneCommit(commitTs)
		return commitTs, err
	}
	return ret, nil
}

func (t *Transaction) Discard() {
	if t.discard {
		return
	}
	if t.numIterators.Load() > 0 {
		panic("Unclosed iterator at time of Txn.Discard.")
	}
	t.discard = true
	t.db.transactionManager.doneStart(t)
	for _, en := range t.pendingKeys {
		en.Release()
	}
	// 两个 map 均已池化复用: pendingKeys 无外部引用; conflictKeys 在提交时已快照, 外部不再持有;
	clear(t.pendingKeys)
	clear(t.conflictKeys)
	// 提交路径取出的 key+8 缓冲块, 在请求落盘后不再被引用, 归还池中复用;
	for _, b := range t.keyBufs {
		model.KeyTsBufPool.Put(b)
	}
	t.keyBufs = t.keyBufs[:0]
	// 归还池中;
	t.db.txnPool.Put(t)
}

func (t *Transaction) RollBack() {
	// 归还池化的 entry, 避免从 EntryPool 借出的对象随事务废弃而泄漏;
	for _, en := range t.pendingKeys {
		en.Release()
	}
	t.pendingKeys = nil
	t.conflictKeys = nil
	t.readKeys = nil
	t.db = nil
}

func (db *TrainKV) Update(fn func(txn *Transaction) error) error {
	if db.IsClosed() {
		return common.ErrClosedDB
	}
	localTxn := db.NewTransaction(true)
	defer localTxn.Discard()
	if err := fn(localTxn); err != nil {
		return err
	}
	_, err := localTxn.Commit()
	return err
}

func (db *TrainKV) View(fn func(txn *Transaction) error) error {
	if db.IsClosed() {
		return common.ErrClosedDB
	}
	localTxn := db.NewTransaction(false)
	defer localTxn.Discard()

	return fn(localTxn)
}

type pendingWritesIterator struct {
	entries   []*model.Entry
	nextIndex int
	startTs   uint64
	reversed  bool
}

func (t *Transaction) newPendingWritesIterator(reversed bool) *pendingWritesIterator {
	if !t.update || len(t.pendingKeys) == 0 {
		return nil
	}
	entries := make([]*model.Entry, 0, len(t.pendingKeys))
	for _, entry := range t.pendingKeys {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &pendingWritesIterator{
		entries:  entries,
		startTs:  t.startTs,
		reversed: reversed,
	}
}
func (pi *pendingWritesIterator) Name() string {
	return "pendingWritesIterator"
}
func (pi *pendingWritesIterator) Next() {
	pi.nextIndex++
}
func (pi *pendingWritesIterator) Valid() bool {
	return pi.nextIndex < len(pi.entries)
}
func (pi *pendingWritesIterator) Rewind() {
	pi.nextIndex = 0
}
func (pi *pendingWritesIterator) Seek(keyStartTs []byte) {
	rawKey := model.ParseKey(keyStartTs)
	pi.nextIndex = sort.Search(len(pi.entries), func(i int) bool {
		cmp := bytes.Compare(pi.entries[i].Key, rawKey)
		if !pi.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}
func (pi *pendingWritesIterator) Item() interfaces.Item {
	utils.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIndex]
	safeCopy := entry.SafeCopy()
	safeCopy.Key = model.KeyWithTs(safeCopy.Key, pi.startTs)
	safeCopy.Version = pi.startTs
	return interfaces.Item{Item: safeCopy}
}
func (pi *pendingWritesIterator) Close() error {
	return nil
}
func (pi *pendingWritesIterator) key() []byte {
	utils.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIndex]
	return model.KeyWithTs(entry.Key, pi.startTs)
}
