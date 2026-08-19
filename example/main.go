package main

import (
	"bytes"
	"fmt"

	"github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/utils"
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

	key := []byte("key1")
	val := []byte("value1")

	txn1 := db.NewTransaction(true)

	// t1 set key1.
	if err = txn1.Set(key, val); err != nil {
		panic(err)
	}

	// t1 update key1 again.
	val2 := []byte("value1_1")
	if err = txn1.Set(key, val2); err != nil {
		panic(err)
	}

	txn2 := db.NewTransaction(true)
	// t2 set key2,value2.
	if err = txn2.Set([]byte("Key2"), []byte("value2")); err != nil {
		panic(err)
	}
	commit2, err := txn2.Commit()
	if err != nil {
		panic(err)
	}
	fmt.Printf("txn2.Commit(), commits=%d;\n", commit2)

	// t1 get key1.
	entry, _ := txn1.Get(key)
	utils.AssertTrue(bytes.Equal(entry.Value, val2))

	// t1 Delete key1.
	if err := txn1.Delete(key); err != nil {
		panic(err)
	}
	fmt.Printf("txn1.delete(%s);\n", key)

	// t1 get key1 again.
	_, err = txn1.Get(key)
	utils.AssertTrue(err == common.ErrKeyNotFound)

	// Iterator keys(Only valid values are returned).
	iter := txn1.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: true})
	defer func() { err = iter.Close() }()
	iter.Rewind()
	for iter.Valid() {
		it := iter.Item()
		if it.Item.Version != 0 {
			fmt.Printf("txn1.Iterator key=%s, value=%s, meta:%d, version=%d;\n",
				model.ParseKey(it.Item.Key), it.Item.Value, it.Item.Meta, it.Item.Version)
		}
		iter.Next()
	}
	commitTs, err := txn1.Commit()
	if err != nil {
		panic(err)
	}
	fmt.Printf("txn1.Commit(), commits=%d;\n", commitTs)

	txn3 := db.NewTransaction(true)
	if err = txn3.Set([]byte("Key2"), []byte("value3")); err != nil {
		panic(err)
	}

	// Iterator keys(Only valid values are returned).
	iter3 := txn3.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: true})
	defer func() { err = iter3.Close() }()
	iter3.Rewind()
	for iter3.Valid() {
		entry := iter3.Item().Item
		if entry.Version != 0 {
			fmt.Printf("txn3.Iterator key=%s, value=%s, meta:%d, version=%d;\n",
				model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
		}
		iter3.Next()
	}
	txn3.RollBack()
}
