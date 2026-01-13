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

	// Transaction operations
	txn := db.NewTransaction(true) // Start an update transaction
	defer txn.Discard() // Ensure the transaction is discarded

	// Set key-value pairs in transaction
	err = txn.Set([]byte("txn_key"), []byte("txn_value"))
	if err != nil {
		fmt.Printf("Transaction set failed: %v\n", err)
	}

	// Get value in transaction
	entry, err = txn.Get([]byte("txn_key"))
	if err != nil {
		fmt.Printf("Transaction get failed: %v\n", err)
	} else {
		fmt.Printf("In transaction key=%s, value=%s\n", entry.Key, entry.Value)
	}

	// Commit transaction
	commitTs, err := txn.Commit()
	if err != nil {
		fmt.Printf("Transaction commit failed: %v\n", err)
	} else {
		fmt.Printf("Transaction committed successfully, commit timestamp: %d\n", commitTs)
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