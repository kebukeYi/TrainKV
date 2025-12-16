package TrainKV

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
)

type DBIterator struct {
	iter      model.Iterator
	vlog      *ValueLog
	lastKey   []byte
	lastEntry model.Entry
}

func (db *TrainKV) NewDBIterator(opt *model.Options) *DBIterator {
	iters := make([]model.Iterator, 0)
	iters = append(iters, db.Lsm.NewLsmIterator(opt)...)
	res := &DBIterator{
		iter: lsm.NewMergeIterator(iters, !opt.IsAsc),
		vlog: db.vlog,
	}
	return res
}
func (dbIter *DBIterator) Name() string {
	return "DBIterator"
}
func (dbIter *DBIterator) Next() {
	dbIter.iter.Next()
}
func (dbIter *DBIterator) Seek(key []byte) {
	dbIter.iter.Seek(key)
}
func (dbIter *DBIterator) Rewind() {
	dbIter.iter.Rewind()
}
func (dbIter *DBIterator) Valid() bool {
	return dbIter.iter.Valid() || dbIter.lastEntry.Key != nil
}
func (dbIter *DBIterator) Item() model.Item {
	judgeKey := func(entry model.Entry) model.Item {
		if entry.Value != nil && model.IsValPtr(&entry) {
			var vp model.ValuePtr
			vp.Decode(entry.Value)
			read, callback, err := dbIter.vlog.Read(&vp)
			model.RunCallback(callback)
			if err != nil {
				fmt.Printf("dbIter read Item()value error: %v", err)
				return model.Item{Item: model.Entry{Version: 0}}
			}
			entry.Value = model.SafeCopy(nil, read)
		}

		if entry.IsDeleteOrExpired() {
			//fmt.Printf("entry is deleted or expired, key:%s, len(val):%d, version:%d, Meat:%d ;\n", model.ParseKey(entry.Key), len(entry.Value), entry.Version, entry.Meta)
			return model.Item{Item: model.Entry{Version: 0}}
		}

		return model.Item{Item: entry}
	}
	// 1) sst [aaa:4]
	// 2) sst [aaa:4,aaa:7,aaa:8,aaa:14]
	// 3) sst [aaa:4,aaa:7,bbb:8,bbb:14]
	//for ; dbIter.Valid(); dbIter.Next() {
	for ; dbIter.iter.Valid(); dbIter.iter.Next() {
		entry := dbIter.iter.Item().Item
		curKey := entry.Key

		if dbIter.lastKey == nil {
			dbIter.lastKey = model.SafeCopy(dbIter.lastKey, curKey)
			dbIter.lastEntry = entry
		}

		if !model.SameKeyNoTs(curKey, dbIter.lastKey) {
			lastEntry := dbIter.lastEntry
			dbIter.lastKey = model.SafeCopy(dbIter.lastKey, curKey)
			dbIter.lastEntry = entry
			return judgeKey(lastEntry)
		} else {
			if model.ParseTsVersion(curKey) > model.ParseTsVersion(dbIter.lastKey) {
				dbIter.lastKey = model.SafeCopy(dbIter.lastKey, curKey)
				dbIter.lastEntry = entry
			} else {
				// model.ParseTsVersion(curKey) < model.ParseTsVersion(lastKey)
				// don`t do anything; continue to next key;
			}
		}
	}
	if dbIter.lastEntry.Key != nil {
		lastValidEntry := dbIter.lastEntry
		dbIter.lastEntry = model.Entry{Version: 0}
		dbIter.lastKey = nil
		return judgeKey(lastValidEntry)
	}
	return model.Item{Item: model.Entry{Version: 0}}
}
func (dbIter *DBIterator) Close() error {
	return dbIter.iter.Close()
}
