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

TrainKV 是一个基于 LSM-Tree 架构的轻量级支持kv分离的嵌入式 Key-Value 存储引擎。

## 特性

- **LSM-Tree 存储引擎** - 多层压缩，支持 L0-L7 级别
- **SkipList MemTable** - 基于 Arena 分配器的无锁跳表
- **KV 分离** - 大值存储在 Value Log 中，降低写放大
- **W-TinyLFU 缓存** - 自适应缓存，结合布隆过滤器 + Count-Min Sketch
- **Mmap I/O** - 内存映射文件，高效随机读取
- **崩溃恢复** - WAL + CRC32 校验 + Manifest 元数据
- **Value Log GC** - 基于废弃比例的自动垃圾回收
- **事务支持** - 支持 ACID 特性的事务操作，可选冲突检测

## 安装

```sh
go get github.com/kebukeYi/TrainKV/v2@latest
```

## 快速开始

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

## 架构

```
┌─────────────────────────────────────────┐
│              TrainKV API                │
├─────────────────────────────────────────┤
│  MemTable (SkipList)  │   Value Log     │
├───────────────────────┼─────────────────┤
│      不可变 MemTables (队列)             │
├─────────────────────────────────────────┤
│            LSM-Tree 层级                │
│  L0 → L1 → L2 → ... → L7 (SSTable)      │
├─────────────────────────────────────────┤
│   Mmap 文件 I/O  │  WAL  │  Manifest    │
└─────────────────────────────────────────┘
```