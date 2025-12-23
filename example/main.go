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

	key := []byte("key")
	val := []byte("value1")

	txn1 := db.NewTransaction(true)

	// set key.
	if err = txn1.Set(key, val); err != nil {
		panic(err)
	}

	// update key again.
	val2 := []byte("value2")
	if err = txn1.Set(key, val2); err != nil {
		panic(err)
	}

	txn2 := db.NewTransaction(true)
	// To test a valid key.
	if err = txn2.Set([]byte("newKey"), []byte("newValue")); err != nil {
		panic(err)
	}
	_, err = txn2.Commit()
	if err != nil {
		panic(err)
	}

	// get key.
	if entry, err := txn1.Get(key); err != nil || entry == nil {
		fmt.Printf("err:%v; txn.get(key): %s;\n", err, key)
	} else {
		fmt.Printf("txn.get(%s), value=%s, meta:%d, version=%d;\n",
			model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
	}

	// Delete key.
	if err := txn1.Delete(key); err != nil {
		panic(err)
	}

	// get key again.
	if entry, err := txn1.Get(key); err != nil || entry == nil {
		fmt.Printf("err: %v; txn.get(%s);\n", err, key)
	} else {
		fmt.Printf("txn.get(%s), value=%s, meta:%d, version=%d;\n",
			model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
	}

	// Iterator keys(Only valid values are returned).
	iter := txn1.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: true})
	defer func() { err = iter.Close() }()
	iter.Rewind()
	for iter.Valid() {
		it := iter.Item()
		if it.Item.Version != 0 {
			fmt.Printf("txn.Iterator key=%s, value=%s, meta:%d, version=%d;\n", model.ParseKey(it.Item.Key), it.Item.Value, it.Item.Meta, it.Item.Version)
		}
		iter.Next()
	}
	commitTs, err := txn1.Commit()
	if err != nil {
		panic(err)
	}
	fmt.Printf("txn.Commit(), commitTs=%d;\n", commitTs)
}
