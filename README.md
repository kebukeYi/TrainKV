<div align="center">
<strong>
<samp>

[English](https://github.com/kebukeYi/TrainKV/blob/main/README.md) · [简体中文](https://github.com/kebukeYi/TrainKV/blob/main/README_CN.md)

</samp>
</strong>
</div>

# TrainKV

[![Go](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

TrainKV is a lightweight embedded Key-Value storage engine based on LSM-Tree architecture with key-value separation support.

## Features

- **LSM-Tree Storage Engine** - Multi-level compaction with L0-L7 levels
- **SkipList MemTable** - Lock-free skip list with Arena allocator
- **KV Separation** - Large values stored in Value Log to reduce write amplification
- **W-TinyLFU Cache** - Adaptive cache with Bloom Filter + Count-Min Sketch
- **Mmap I/O** - Memory-mapped file for efficient random reads
- **Crash Recovery** - WAL + CRC32 checksum + Manifest metadata
- **Value Log GC** - Automatic garbage collection based on discard ratio
- **Transaction Support** - ACID-compliant transaction operations with optional conflict detection

## Installation

```sh
go get github.com/kebukeYi/TrainKV/v2@latest
```

## Quick Start

```go
package main

import (
	"fmt"

	"github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/kebukeYi/TrainKV/v2/model"
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

```

## Architecture

```
┌─────────────────────────────────────────┐
│              TrainKV API                │
├─────────────────────────────────────────┤
│  MemTable (SkipList)  │   Value Log     │
├───────────────────────┼─────────────────┤
│      Immutable MemTables (Queue)        │
├─────────────────────────────────────────┤
│            LSM-Tree Levels              │
│  L0 → L1 → L2 → ... → L7 (SSTable)      │
├─────────────────────────────────────────┤
│   Mmap File I/O  │  WAL  │  Manifest    │
└─────────────────────────────────────────┘
```