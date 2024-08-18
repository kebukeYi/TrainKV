package lsm

import (
	"fmt"
	"trainKv/model"
)

type DBIterator struct {
	iter model.Iterator
	vlog *ValueLog
}

func (db *TrainKVDB) NewDBIterator(opt *model.Options) *DBIterator {
	iters := make([]model.Iterator, 0)
	iters = append(iters, db.lsm.NewLsmIterator()...)
	res := &DBIterator{
		iter: nil,
		vlog: db.vlog,
	}
	return res
}

func (dbIter *DBIterator) Next() {
	dbIter.iter.Next()
	for ; dbIter.Valid() && dbIter.Item().Item == nil; dbIter.iter.Next() {
	}
}

func (dbIter *DBIterator) Valid() bool {
	return dbIter.iter.Valid()
}

func (dbIter *DBIterator) Seek(key []byte) {
	dbIter.iter.Seek(key)
}
func (dbIter *DBIterator) Rewind() {
	dbIter.iter.Rewind()
	for ; dbIter.Valid() && dbIter.Item().Item == nil; dbIter.iter.Next() {
	}
}
func (dbIter *DBIterator) Item() model.Item {
	entry := dbIter.iter.Item().Item
	var value []byte
	if entry != nil && model.IsValPtr(entry) {
		var vp *model.ValuePtr
		vp.Decode(entry.Value)
		read, callback, err := dbIter.vlog.read(vp)
		defer model.RunCallback(callback)
		if err != nil {
			fmt.Printf("dbIter read Item()value error: %v", err)
			return model.Item{Item: nil}
		}
		value = model.SafeCopy(nil, read)
	}
	if entry.IsDeleteOrExpired() || value == nil {
		return model.Item{Item: nil}
	}
	ret := &model.Entry{
		Key:       entry.Key,
		Value:     value,
		ExpiresAt: entry.ExpiresAt,
		Meta:      entry.Meta,
		Version:   entry.Version,
		Offset:    entry.Offset,
	}
	return model.Item{Item: ret}
}
func (dbIter *DBIterator) Close() error {
	return dbIter.iter.Close()
}
