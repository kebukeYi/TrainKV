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
	"github.com/kebukeYi/TrainKV"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
)

func main() {
	// 打开数据库（空路径会创建临时目录）
	db, err, cleanup := TrainKV.Open(lsm.GetLSMDefaultOpt(""))
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
		_ = cleanup()
	}()

	// 写入
	_ = db.Set(model.NewEntry([]byte("hello"), []byte("world")))

	// 读取
	entry, _ := db.Get([]byte("hello"))
	fmt.Printf("key=%s, value=%s\n", entry.Key, entry.Value)

	// 删除
	_ = db.Del([]byte("hello"))

	// 迭代器
	iter := db.NewDBIterator(&model.Options{IsAsc: true})
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		fmt.Printf("key=%s, value=%s\n", model.ParseKey(it.Item.Key), it.Item.Value)
	}

	// 事务操作
	txn := db.NewTransaction(true) // 开始一个更新事务
	defer txn.Discard() // 确保事务被丢弃

	// 在事务中设置键值对
	err = txn.Set([]byte("txn_key"), []byte("txn_value"))
	if err != nil {
		fmt.Printf("事务设置失败: %v\n", err)
	}

	// 在事务中获取值
	entry, err = txn.Get([]byte("txn_key"))
	if err != nil {
		fmt.Printf("事务获取失败: %v\n", err)
	} else {
		fmt.Printf("事务中 key=%s, value=%s\n", entry.Key, entry.Value)
	}

	// 提交事务
	commitTs, err := txn.Commit()
	if err != nil {
		fmt.Printf("事务提交失败: %v\n", err)
	} else {
		fmt.Printf("事务提交成功，提交时间戳: %d\n", commitTs)
	}
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