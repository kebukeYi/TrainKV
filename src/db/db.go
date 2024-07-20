package db

import (
	"math"
	"sync"
	"trainKv/common"
	"trainKv/lsm"
	"trainKv/model"
)

type Options struct {
	ValueThreshold      int64
	WorkDir             string
	MemTableSize        int64
	SSTableSize         int64
	MaxBatchCount       int64
	MaxBatchSize        int64
	ValueLogFileSize    int64
	VerifyValueChecksum bool
	ValueLogMaxEntries  uint32
	LogRotatesToFlush   int32
	MaxTableSize        int64
}

type TrainKVDB struct {
	mux         sync.Mutex
	lsm         *lsm.LSM
	vlog        *lsm.ValueLog
	opt         Options
	writeCh     chan *lsm.Request
	blockWrites int32
	vhead       *model.ValuePtr
	logRotates  int32
}

var (
	GCHead = []byte("!corekv_vlog_head")
)

func Open(opt Options) (*TrainKVDB, error) {
	db := &TrainKVDB{opt: opt}
	db.initVLog()
	db.lsm = lsm.NewLSM(&lsm.Options{
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
	db.writeCh = make(chan *lsm.Request)
	go db.getFromWriteCh()
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
	if entry != nil && lsm.IsDeletedOrExpired(entry) {
		return nil, common.ErrKeyNotFound
	}
	if entry != nil && model.IsValPtr(entry) {
		var vp *model.ValuePtr
		vp.Decode(entry.Value)
		read, callBack, err := db.vlog.Read(vp)
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

func (db *TrainKVDB) BatchSet(entries []*model.Entry) error {
	return nil
}

func (db *TrainKVDB) sendToWriteCh(entries []*model.Entry) (*lsm.Request, error) {
	return nil, nil
}

func (db *TrainKVDB) getFromWriteCh() {

}

func (db *TrainKVDB) writeRequest(reqs *[]lsm.Request) error {
	return nil
}

func (db *TrainKVDB) writeToLSM(req *lsm.Request) error {
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
