# TrainKV

[![Go](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

TrainKV 是一个基于 LSM-Tree 架构的高性能嵌入式 Key-Value 存储引擎，使用 Go 语言从零实现。

## 特性

- **LSM-Tree 存储引擎** - 多层压缩，支持 L0-L7 级别
- **SkipList MemTable** - 基于 Arena 分配器的无锁跳表
- **KV 分离** - 大值存储在 Value Log 中，降低写放大
- **W-TinyLFU 缓存** - 自适应缓存，结合布隆过滤器 + Count-Min Sketch
- **Mmap I/O** - 内存映射文件，高效随机读取
- **崩溃恢复** - WAL + CRC32 校验 + Manifest 元数据
- **Value Log GC** - 基于废弃比例的自动垃圾回收

## 安装

```sh
go get github.com/kebukeYi/TrainKV@latest
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