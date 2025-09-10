# TrainKV

TrainKV is a key-value store that uses LSM-trees to store data. It is designed to be fast, efficient, and easy to use.

## Getting Started

### Installing

```sh
go get github.com/kebukeYi/TrainKV@latest
```

### Basic operations

```go
package main

import (
	"fmt"
	"github.com/kebukeYi/TrainKV"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
)

func main() {
	// 未指定具体工作目录时, 程序会创建临时目录, 程序正常关闭时会清理临时目录;
	dirPath := ""
	defaultOpt := lsm.GetLSMDefaultOpt(dirPath)
	db, err, callBack := TrainKV.Open(defaultOpt)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()

	key := "Name"
	val := "TrainKV"

	// Set key.
	e := model.NewEntry([]byte(key), []byte(val))
	if err := db.Set(e); err != nil {
		panic(err)
	}

	// To test a valid key for the following iterator.
	newE := model.NewEntry([]byte("newName"), []byte("newValidVal"))
	if err := db.Set(newE); err != nil {
		panic(err)
	}

	// Get key.
	if entry, err := db.Get([]byte(key)); err != nil || entry == nil {
		fmt.Printf("err: %v; db.Get key=%s;\n", err, key)
	} else {
		fmt.Printf("db.Get key=%s, value=%s, meta:%d, version=%d; \n",
			model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
	}

	// Delete key.
	if err := db.Del([]byte(key)); err != nil {
		panic(err)
	}

	// Get key again.
	if entry, err := db.Get([]byte(key)); err != nil || entry == nil {
		fmt.Printf("err: %v; db.Get key=%s;\n", err, key)
	} else {
		fmt.Printf("db.Get key=%s, value=%s, meta:%d, version=%d; \n",
			model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
	}

	// Iterator keys(Only valid values are returned).
	iter := db.NewDBIterator(&model.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	iter.Rewind()
	for iter.Valid() {
		it := iter.Item()
		if it.Item.Version != -1 {
			fmt.Printf("db.Iterator key=%s, value=%s, meta:%d, version=%d\n;",
				model.ParseKey(it.Item.Key), it.Item.Value, it.Item.Meta, it.Item.Version)
		}
		iter.Next()
	}
}

```
