# TrainKV

A lightweight embedded key-value store in pure Go, built on the LSM-tree architecture with value-log separation — inspired by [Badger](https://github.com/dgraph-io/badger).

TrainKV is designed for learning and practical use: a compact, readable codebase (~10k LOC, no CGO) with production-grade concerns — MVCC transactions, group commit, block cache, bloom filters, background compaction and value-log garbage collection — all covered by end-to-end tests and race-detector verification.

## Features

- **LSM-tree storage**: write-optimized memtable (skip list + WAL) flushing to sorted SSTables (L0–L6), with background compaction.
- **Value-log separation**: values above `ValueThreshold` are stored in a value log (vlog); the LSM keeps a 12-byte pointer. Reads of big values go through mmap with lazy resolution.
- **MVCC transactions**: timestamp-ordered read/write transactions with conflict detection (optional), read-your-writes, and safe version reclamation.
- **Group commit**: concurrent writers are batched through a write channel, amortizing fsync across requests.
- **Block cache (W-TinyLFU)**: adaptive hot/cold caching of SST blocks; bloom filters prune reads.
- **Crash safety**: WAL replay, MANIFEST-based table tracking, vlog value pointers are synced before WAL.
- **vlog GC**: dead value reclamation driven by compaction-generated discard statistics.

## Architecture

```
┌──────────────────────────────  TrainKV  ──────────────────────────────┐
│                                                                       │
│  Transaction API ── writeCh (group commit) ── handleWriteCh           │
│        │                                       │                      │
│        │ txn.Set/Get/Delete                    ├─ vlog.Write (big values)
│        │                                       ├─ LSM.Put              │
│        ▼                                       │   ├─ WAL + skip list  │
│  LSM ── memtable ──flush──► SST (L0) ──compaction──► L1..L6            │
│        │  ▲                    │  ▲                                   │
│        │  │ block cache        │  │ bloom filter + block index        │
│        ▼  └────────────────────┘  └───────────────────┐               │
│  MANIFEST ── table metadata        compaction ── discard stats ──► vlog GC
└───────────────────────────────────────────────────────────────────────┘
```

Key components:

| Component | Role |
|---|---|
| `skl` | Lock-free skip list + arena allocator for the memtable |
| `lsm` | Memtable/WAL, SST build & read (block index, bloom), L0–L6 compaction |
| `utils/cache` | W-TinyLFU block cache (win-LRU + segmented-LRU + CMSketch) |
| `vlog` | Value log: append-only big-value storage, GC by discard ratio |
| `MANIFEST` | Persistent table registry for crash recovery |
| `txn` | MVCC transactions: timestamps, conflict detection, read-your-writes |

## Quick start

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
	// Empty dir -> a temp dir is created and cleaned up on Close.
	opt := lsm.GetDefaultOpt("")
	db, err, callBack := TrainKV.Open(opt)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()

	// Write transaction.
	txn := db.NewTransaction(true)
	if err := txn.Set([]byte("key1"), []byte("value1")); err != nil {
		panic(err)
	}
	if _, err := txn.Commit(); err != nil {
		panic(err)
	}

	// Read transaction.
	rtxn := db.NewTransaction(false)
	defer rtxn.Discard()
	entry, err := rtxn.Get([]byte("key1"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("key1 = %s\n", entry.Value)

	// Iterate (only visible, non-deleted entries are returned).
	iter := rtxn.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: true})
	defer func() { _ = iter.Close() }()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		fmt.Printf("%s = %s\n", model.ParseKey(it.Item.Key), it.Item.Value)
	}

	// Delete.
	dtxn := db.NewTransaction(true)
	if err := dtxn.Delete([]byte("key1")); err != nil {
		panic(err)
	}
	if _, err := dtxn.Commit(); err != nil {
		panic(err)
	}

	// vlog GC: reclaim dead big values (driven by compaction stats).
	_ = db.RunValueLogGC(0.5)
}
```

See `example/main.go` for a runnable version.

## API overview

| API | Description |
|---|---|
| `TrainKV.Open(opt)` | Open (or create) a database; returns `(db, err, callBack)` — the callback removes the temp dir if `opt.WorkDir` was empty |
| `db.NewTransaction(update bool)` | Start a read (`false`) or write (`true`) transaction |
| `txn.Set / Get / Delete` | Data operations |
| `txn.Commit() / Discard() / RollBack()` | Finish a transaction |
| `txn.NewIterator(&interfaces.Options{...})` | Scan with MVCC visibility |
| `db.RunValueLogGC(ratio)` | Trigger vlog garbage collection |
| `db.BatchSet / db.WriteRequest` | Lower-level batched write API |

Key options (`lsm.GetDefaultOpt`): `MemTableSize` (64MB), `ValueThreshold` (1MB — values above go to vlog), `SyncWrites` (true — fsync vlog + WAL per batch), `BlockSize` (4KB), `CacheNums` (10240), `NumLevelZeroTables` (5), `MaxLevelNum` (7), `DetectConflicts` (true).

## Contracts

These contracts keep the hot paths allocation-free and race-free. Please follow them:

1. **Transactions**: a transaction must not be used after `Commit()` or `Discard()` (it is pooled and may be reused by another caller).
2. **Iterator items are borrowed**: the `Item` returned by `iter.Item()` is valid only until the next `Next()`/`Rewind()`/`Seek()`. `Rewind()` invalidates all previously returned items (the backing arena is reset).
3. **Big values are lazy**: for entries stored in the vlog, `Item.Item.Value` is `nil` — call `iter.Item().Value()` to resolve, and consume it before the next `Next()`.

## Performance

Measured on an Intel i5-7300HQ (WSL2), Go 1.25, `-benchtime=2s -count=3` (medians). `SyncWrites=true` (production config); allocs/op is the stable metric — ns/op on the write path is fsync-dominated.

### Write path

| Benchmark | Scenario | ns/op | allocs/op |
|---|---|---|---|
| WriteTxnSet | serial single txn, 128B | ~3.8ms | **5** |
| TrainKVTxnSetParallel | 4 goroutines | **~2.1ms** | **8** |
| TrainKVBatchSet10 | 10 entries/txn | ~4.7ms | 64 |
| TrainKVTxnSetBigValue | 1MB value (kv-separated) | ~25ms | 8 |
| WriteRequest | low-level batch API | ~4.0ms | 15 |

### Read path

| Benchmark | Scenario | ns/op | allocs/op |
|---|---|---|---|
| ReadGetHitSeq | memtable hit | **~1.2µs** (0.8M ops/s) | 2 |
| ReadGetMiss | not found | ~0.9µs | 2 |
| ReadIterate | scan 100K keys (memtable) | ~35ms | **832** (was 200,000) |
| ReadIterateSST | scan 100K keys (SST) | ~26ms | **0** |
| ReadIterateBigValue | scan 500 × 1MB (lazy) | ~110µs | **3** (no MB copies) |

The read path is allocation-free on scans (chunked arena) and resolves big values on demand instead of copying them eagerly.

## Testing

All five core paths — read, write, compaction, restart/recovery, vlog GC — have end-to-end tests, and the full suite passes under `-race`:

- `TestFlushReadIntegrity` — 100K keys read back after flush: 0 missing
- `TestDBAutoCompaction` — 60K writes + updates + deletes → compaction → full read-back, with concurrent reads
- `TestValueLogGCFull` — vlog GC end-to-end: stats → pick → rewrite → delete old file → reopen
- `TestCompactionL0ToL0 / L0ToLmax / TestLastLevelCompaction` — all three compaction paths
- `TestCrashRecovery` — WAL replay after simulated power loss

```bash
go test ./... -race -count=1          # full suite with race detector
go test ./benchmk/ -bench=. -benchmem -benchtime=2s -count=3   # benchmarks
```

## Documentation

- `docs/improvements-summary.md` — plain-language summary of the optimization & bug-fix campaign
- `docs/improvements-detail.md` — code-level details of every change
- `docs/todo-list.md` — prioritized backlog (crash-recovery deep scenarios, soak tests, remaining perf items)
- `docs/write-path-optimization.md` — the original write-path optimization report

## License

MIT — see [LICENSE](LICENSE).
