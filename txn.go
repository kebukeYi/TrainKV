package TrainKV

import (
	"bytes"
	"context"
	"encoding/hex"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/utils"
	"github.com/pkg/errors"
	"sync"
	"sync/atomic"
)

type TransactionManager struct {
	detectConflicts bool
	tsLock          sync.Mutex
	writeChLock     sync.Mutex
	nextTxnTs       uint64
	startMark       *utils.LimitMark
	commitMark      *utils.LimitMark
	commitedTxns    []commitedTxn
	lastCleanupTs   uint64
	closer          *utils.Closer
}

type commitedTxn struct {
	TxnID        uint64
	conflictKeys map[uint64]struct{}
}

func NewTransactionManager(options *lsm.Options) *TransactionManager {
	tm := &TransactionManager{
		detectConflicts: options.DetectConflicts,
		startMark:       &utils.LimitMark{Name: "startMark"},
		commitMark:      &utils.LimitMark{Name: "commitMark"},
		closer:          utils.NewCloser(2),
	}
	tm.startMark.Init(tm.closer)
	tm.commitMark.Init(tm.closer)
	return tm
}
func (m *TransactionManager) Stop() {
	m.closer.CloseAndWait()
}
func (m *TransactionManager) startTs() uint64 {
	m.tsLock.Lock()
	startTs := m.nextTxnTs - 1
	m.startMark.Begin(startTs)
	m.tsLock.Unlock()
	err := m.commitMark.WaitForIndexDone(context.Background(), startTs)
	common.Check(err)
	return startTs
}
func (m *TransactionManager) nextTs() uint64 {
	m.tsLock.Lock()
	defer m.tsLock.Unlock()
	return m.nextTxnTs
}
func (m *TransactionManager) incrementNextTs() {
	m.tsLock.Lock()
	m.nextTxnTs++
	m.tsLock.Unlock()
}
func (m *TransactionManager) discardTs() uint64 {
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
		if txn.startTs >= commit.TxnID {
			continue
		}
		// txn.startTs < commit.TxnID
		for _, key := range txn.readKeys {
			if _, ok := commit.conflictKeys[key]; ok {
				return true
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
	m.cleanCommitedTransaction()
	commitTs = m.nextTxnTs
	m.nextTxnTs++
	m.commitMark.Begin(commitTs)
	utils.AssertTrue(commitTs >= m.lastCleanupTs)
	if m.detectConflicts {
		m.commitedTxns = append(m.commitedTxns, commitedTxn{
			TxnID:        commitTs,
			conflictKeys: txn.conflictKeys,
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
		if txn.TxnID <= maxStartTs {
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
	pendingKeys  map[string]*model.Entry
	conflictKeys map[uint64]struct{}
	count        int64
	size         int64
	db           *TrainKV
	commitTs     uint64
	numIterators atomic.Int32
	startDone    bool
	update       bool
	discard      bool
}

func (db *TrainKV) NewTransaction(update bool) *Transaction {
	txn := &Transaction{
		db:     db,
		update: update,
		count:  1,
		size:   int64(len(common.TxnKey) + 10),
	}
	if update {
		if db.Opt.DetectConflicts {
			txn.conflictKeys = make(map[uint64]struct{})
		}
		txn.pendingKeys = make(map[string]*model.Entry)
	}
	txn.startTs = db.transactionManager.startTs()
	return txn
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

	if t.db.Opt.DetectConflicts {
		hash, _ := utils.KeyToHash(e.Key)
		t.conflictKeys[hash] = struct{}{}
	}

	t.pendingKeys[string(e.Key)] = e
	return nil
}
func exceedsSize(prefix string, max int64, key []byte) error {
	return errors.Errorf("%s with size %d exceeded %d limit. %s:\n%s", prefix, len(key), max, prefix, hex.Dump(key[:1<<10]))
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
func (t *Transaction) Get(key []byte) (*model.Entry, error) {
	if len(key) == 0 {
		return nil, common.ErrEmptyKey
	} else if t.discard {
		return nil, common.ErrDiscardedTxn
	}
	if t.update {
		if e, ok := t.pendingKeys[string(key)]; ok && bytes.Equal(e.Key, key) {
			if model.IsDeletedOrExpired(e.Meta, e.ExpiresAt) {
				return nil, common.ErrKeyNotFound
			}
			e.Version = t.startTs
			entry := e.SafeCopy()
			return &entry, nil
		}
		t.addReadKey(key)
	}
	keyMaxReadTs := model.KeyWithTs(key, t.startTs)
	entry, err := t.db.Get(keyMaxReadTs)
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
	entry := &model.Entry{
		Key:  key,
		Meta: common.BitDelete,
	}
	return t.modify(entry)
}
func (t *Transaction) addReadKey(key []byte) {
	if t.update {
		hash, _ := utils.KeyToHash(key)
		t.readKeys = append(t.readKeys, hash)
	}
}

func (t *Transaction) Commit() error {
	defer t.Discard()
	callBack, err := t.commitAndSendToDB()
	if err != nil {
		return err
	}
	err = callBack()
	return err
}

func (t *Transaction) commitAndSendToDB() (func() error, error) {
	manager := t.db.transactionManager
	manager.writeChLock.Lock()
	defer manager.writeChLock.Unlock()
	commitTs, hasConflicts := manager.newCommitTs(t)
	if hasConflicts {
		return nil, common.ErrConflict
	}
	keepTogether := true
	entries := make([]*model.Entry, len(t.pendingKeys))
	for _, entry := range t.pendingKeys {
		if entry.Version == 0 {
			entry.Version = commitTs
		} else {
			keepTogether = false
		}
		entry.Key = model.KeyWithTs(entry.Key, entry.Version)
		if keepTogether {
			entry.Meta |= common.BitTxn
		}
		entries = append(entries, entry)
	}
	req, err := t.db.SendToWriteCh(entries)
	if err != nil {
		manager.doneCommit(commitTs)
		return nil, err
	}
	ret := func() error {
		// 阻塞等待,写入lsm结果, 然后才允许 结束当前水印;
		err := req.Wait()
		manager.doneCommit(commitTs)
		return err
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
}

func (t *Transaction) StartTs() uint64 {
	return t.startTs
}
