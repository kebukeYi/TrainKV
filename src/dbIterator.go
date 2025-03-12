package src

import (
	"fmt"
	"trainKv/lsm"
	"trainKv/model"
)

type DBIterator struct {
	iter    model.Iterator
	vlog    *ValueLog
	skipKey []byte
}

func (db *TrainKVDB) NewDBIterator(opt *model.Options) *DBIterator {
	iters := make([]model.Iterator, 0)
	iters = append(iters, db.Lsm.NewLsmIterator(opt)...)
	res := &DBIterator{
		iter: lsm.NewMergeIterator(iters, !opt.IsAsc),
		vlog: db.vlog,
	}
	return res
}

func (dbIter *DBIterator) Next() {
	dbIter.iter.Next()
	for ; dbIter.Valid() && dbIter.Item().Item.Value == nil; dbIter.iter.Next() {
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
	for ; dbIter.Valid() && dbIter.Item().Item.Key == nil; dbIter.iter.Next() {
	}
}

func (dbIter *DBIterator) Item() model.Item {
	entry := dbIter.iter.Item().Item
	var value []byte

	if entry.Value != nil && model.IsValPtr(entry) {
		var vp model.ValuePtr
		vp.Decode(entry.Value)
		read, callback, err := dbIter.vlog.Read(&vp)
		defer model.RunCallback(callback)
		if err != nil {
			fmt.Printf("dbIter read Item()value error: %v", err)
			return model.Item{Item: model.Entry{Version: -1}}
		}
		value = model.SafeCopy(nil, read)
	}

	if entry.IsDeleteOrExpired() {
		//fmt.Printf("entry is deleted or expired, key:%s, len(val):%d, version:%d, Meat:%d ;\n", model.ParseKey(entry.Key), len(entry.Value), entry.Version, entry.Meta)
		return model.Item{Item: model.Entry{Version: -1}}
	}

	ret := model.Entry{
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
