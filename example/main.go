package main

import (
	"fmt"
	"github.com/kebukeYi/TrainKV"
	"github.com/kebukeYi/TrainKV/interfaces"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
)

func main() {
	// 未指定具体工作目录时, 程序会创建临时目录, 程序正常关闭时会清理临时目录;
	dirPath := ""
	defaultOpt := lsm.GetDefaultOpt(dirPath)
	db, err, callBack := TrainKV.Open(defaultOpt)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()

	keyTs1 := model.KeyWithTs([]byte("name"), 1)
	val1 := "TrainKV-1"

	// Set key.
	e1 := model.NewEntry(keyTs1, []byte(val1))
	if err := db.Set(e1); err != nil {
		panic(err)
	}

	// update key again.
	keyTs2 := model.KeyWithTs([]byte("name"), 2)
	val2 := "TrainKV-2"
	e2 := model.NewEntry(keyTs2, []byte(val2))
	if err := db.Set(e2); err != nil {
		panic(err)
	}

	// To test a valid key for the following iterator.
	newE := model.NewEntry(model.KeyWithTs([]byte("newName"), 1), []byte("newValidVal"))
	if err := db.Set(newE); err != nil {
		panic(err)
	}

	// Get key.
	if entry, err := db.Get(keyTs1); err != nil || entry == nil {
		fmt.Printf("err:%v; db.Get(key): %s;\n", err, keyTs1)
	} else {
		fmt.Printf("db.Get(%s), value=%s, meta:%d, version=%d; \n",
			model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
	}

	// Delete key.
	if err := db.Del(keyTs1); err != nil {
		panic(err)
	}

	// Get key again.
	if entry, err := db.Get(keyTs1); err != nil || entry == nil {
		fmt.Printf("err: %v; db.Get(%s);\n", err, model.ParseKey(keyTs1))
	} else {
		fmt.Printf("db.Get(%s), value=%s, meta:%d, version=%d; \n",
			model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
	}

	// Iterator keys(Only valid values are returned).
	iter := db.NewDBIterator(&interfaces.Options{IsAsc: true, IsSetCache: false})
	defer func() { _ = iter.Close() }()
	iter.Rewind()
	for iter.Valid() {
		it := iter.Item()
		if it.Item.Version != 0 {
			fmt.Printf("db.Iterator key=%s, value=%s, meta:%d, version=%d;\n",
				model.ParseKey(it.Item.Key), it.Item.Value, it.Item.Meta, it.Item.Version)
		}
		iter.Next()
	}
}
