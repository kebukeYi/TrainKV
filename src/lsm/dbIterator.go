package lsm

import (
	"trainKv/interfaces"
)

type DBIterator struct {
	iter interfaces.Iterator
	vlog *ValueLog
}

func (db *TrainKVDB) NewDBIterator(opt *interfaces.Options) *DBIterator {
	iters := make([]interfaces.Iterator, 0)
	iters = append(iters, db.lsm.NewLsmIterator()...)
	res := &DBIterator{
		iter: nil,
		vlog: db.vlog,
	}
	return res
}

func (dbIter *DBIterator) Next() {

}

func (dbIter *DBIterator) Valid() bool {
	return false
}

func (dbIter *DBIterator) Seek(key []byte) {

}
func (dbIter *DBIterator) Rewind() {

}
func (dbIter *DBIterator) Item() interfaces.Item {
	return interfaces.Item{}
}
func (dbIter *DBIterator) Close() error {
	return nil
}
