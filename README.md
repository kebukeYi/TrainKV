# TrainKV

[![Go](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

TrainKV is a high-performance embedded Key-Value storage engine based on LSM-Tree architecture, implemented from scratch in Go.

## Features

- **LSM-Tree Storage Engine** - Multi-level compaction with L0-L7 levels
- **SkipList MemTable** - Lock-free skip list with Arena allocator
- **KV Separation** - Large values stored in Value Log to reduce write amplification
- **W-TinyLFU Cache** - Adaptive cache with Bloom Filter + Count-Min Sketch
- **Mmap I/O** - Memory-mapped file for efficient random reads
- **Crash Recovery** - WAL + CRC32 checksum + Manifest metadata
- **Value Log GC** - Automatic garbage collection based on discard ratio

## Installation

```sh
go get github.com/kebukeYi/TrainKV@latest
```

## Quick Start

```go
package main

import (
	"fmt"
	"github.com/kebukeYi/TrainKV"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
)

func main() {
	// Open database (empty path creates temp directory)
	db, err, cleanup := TrainKV.Open(lsm.GetLSMDefaultOpt(""))
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
		_ = cleanup()
	}()

	// Set
	_ = db.Set(model.NewEntry([]byte("hello"), []byte("world")))

	// Get
	entry, _ := db.Get([]byte("hello"))
	fmt.Printf("key=%s, value=%s\n", entry.Key, entry.Value)

	// Delete
	_ = db.Del([]byte("hello"))

	// Iterator
	iter := db.NewDBIterator(&model.Options{IsAsc: true})
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		fmt.Printf("key=%s, value=%s\n", model.ParseKey(it.Item.Key), it.Item.Value)
	}
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
