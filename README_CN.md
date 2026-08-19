# TrainKV

纯 Go 实现的轻量嵌入式键值存储,基于 LSM-tree 架构 + value 日志分离,参考 [Badger](https://github.com/dgraph-io/badger) 设计。

TrainKV 面向学习与实际使用:代码紧凑可读(约万行,无 CGO),同时具备生产级关注点——MVCC 事务、group commit、块缓存、布隆过滤器、后台压缩与 value 日志垃圾回收,全部由端到端测试与 `-race` 并发检测覆盖。

## 特性

- **LSM-tree 存储**:写优化的内存表(跳表 + WAL)刷盘为有序 SSTable(L0–L6),后台压缩;
- **Value 日志分离**:超过 `ValueThreshold` 的 value 存入 value log(vlog),LSM 只保留 12 字节指针;大 value 读取走 mmap 且**惰性解析**;
- **MVCC 事务**:基于时间戳的读写事务,支持冲突检测(可开关)、读己之写、安全版本回收;
- **Group commit**:并发写者经写通道攒批,摊销 fsync;
- **块缓存(W-TinyLFU)**:自适应冷热分区的 SST 块缓存 + 布隆过滤器剪枝;
- **崩溃安全**:WAL 重放、MANIFEST 表登记、vlog 先于 WAL 同步;
- **vlog GC**:由压缩产生的 discard 统计驱动死数据回收。

## 架构

```
┌──────────────────────────────  TrainKV  ──────────────────────────────┐
│                                                                       │
│  事务 API ── writeCh (group commit) ── handleWriteCh                  │
│        │                                       │                      │
│        │ txn.Set/Get/Delete                    ├─ vlog.Write (大 value)
│        │                                       ├─ LSM.Put              │
│        ▼                                       │   ├─ WAL + 跳表       │
│  LSM ── memtable ──flush──► SST (L0) ──compaction──► L1..L6            │
│        │  ▲                    │  ▲                                   │
│        │  │ 块缓存             │  │ 布隆过滤器 + block 索引           │
│        ▼  └────────────────────┘  └───────────────────┐               │
│  MANIFEST ── 表元数据          压缩 ── discard 统计 ──► vlog GC        │
└───────────────────────────────────────────────────────────────────────┘
```

核心组件:

| 组件 | 职责 |
|---|---|
| `skl` | 无锁跳表 + arena 分配器(内存表) |
| `lsm` | 内存表/WAL、SST 构建与读取(block 索引、布隆)、L0–L6 压缩 |
| `utils/cache` | W-TinyLFU 块缓存(win-LRU + 分段 LRU + CMSketch) |
| `vlog` | 追加式大 value 存储,按 discard 比例 GC |
| `MANIFEST` | 崩溃恢复的表登记文件 |
| `txn` | MVCC 事务:时间戳、冲突检测、读己之写 |

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
	// 空目录 -> 自动创建临时目录, Close 时自动清理;
	opt := lsm.GetDefaultOpt("")
	db, err, callBack := TrainKV.Open(opt)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()

	// 写事务;
	txn := db.NewTransaction(true)
	if err := txn.Set([]byte("key1"), []byte("value1")); err != nil {
		panic(err)
	}
	if _, err := txn.Commit(); err != nil {
		panic(err)
	}

	// 读事务;
	rtxn := db.NewTransaction(false)
	defer rtxn.Discard()
	entry, err := rtxn.Get([]byte("key1"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("key1 = %s\n", entry.Value)

	// 迭代器(只返回可见且未删除的条目);
	iter := rtxn.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: true})
	defer func() { _ = iter.Close() }()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		fmt.Printf("%s = %s\n", model.ParseKey(it.Item.Key), it.Item.Value)
	}

	// 删除;
	dtxn := db.NewTransaction(true)
	if err := dtxn.Delete([]byte("key1")); err != nil {
		panic(err)
	}
	if _, err := dtxn.Commit(); err != nil {
		panic(err)
	}

	// vlog GC:回收死大 value(由压缩统计驱动);
	_ = db.RunValueLogGC(0.5)
}
```

可运行版本见 `example/main.go`。

## API 一览

| API | 说明 |
|---|---|
| `TrainKV.Open(opt)` | 打开(或创建)数据库;返回 `(db, err, callBack)` — `opt.WorkDir` 为空时 callBack 负责清理临时目录 |
| `db.NewTransaction(update bool)` | 开启读(`false`)或写(`true`)事务 |
| `txn.Set / Get / Delete` | 数据操作 |
| `txn.Commit() / Discard() / RollBack()` | 结束事务 |
| `txn.NewIterator(&interfaces.Options{...})` | 带 MVCC 可见性的扫描 |
| `db.RunValueLogGC(ratio)` | 触发 vlog 垃圾回收 |
| `db.BatchSet / db.WriteRequest` | 底层批量写 API |

关键配置(`lsm.GetDefaultOpt`):`MemTableSize`(64MB)、`ValueThreshold`(1MB — 超过则进 vlog)、`SyncWrites`(true — 每批同步 vlog + WAL)、`BlockSize`(4KB)、`CacheNums`(10240)、`NumLevelZeroTables`(5)、`MaxLevelNum`(7)、`DetectConflicts`(true)。

## 使用契约

以下契约保证了热路径的零分配与无竞争,请务必遵守:

1. **事务**:`Commit()` 或 `Discard()` 之后不得再使用该事务(对象会被池化复用);
2. **迭代器 Item 为借用语义**:`iter.Item()` 返回的 Item 仅在下次 `Next()`/`Rewind()`/`Seek()` 之前有效;`Rewind()` 会使之前返回的所有 Item 失效(底层 arena 会重置);
3. **大 value 惰性读取**:vlog 中的条目 `Item.Item.Value` 为 `nil`,需调用 `iter.Item().Value()` 解析,并在下次 `Next()` 前消费。

## 性能

测试环境:Intel i5-7300HQ(WSL2)、Go 1.25、`-benchtime=2s -count=3`(中位数)。`SyncWrites=true`(生产配置);写路径 ns/op 受 fsync 主导,allocs/op 是稳定指标。

### 写路径

| 基准 | 场景 | ns/op | allocs/op |
|---|---|---|---|
| WriteTxnSet | 串行单条 128B | ~3.8ms | **5** |
| TrainKVTxnSetParallel | 4 协程并发 | **~2.1ms** | **8** |
| TrainKVBatchSet10 | 每事务 10 条 | ~4.7ms | 64 |
| TrainKVTxnSetBigValue | 1MB value(kv 分离) | ~25ms | 8 |
| WriteRequest | 底层批量 API | ~4.0ms | 15 |

### 读路径

| 基准 | 场景 | ns/op | allocs/op |
|---|---|---|---|
| ReadGetHitSeq | memtable 命中 | **~1.2µs**(0.8M ops/s) | 2 |
| ReadGetMiss | 未命中 | ~0.9µs | 2 |
| ReadIterate | 扫描 10 万条(memtable) | ~35ms | **832**(原 200,000) |
| ReadIterateSST | 扫描 10 万条(SST) | ~26ms | **0** |
| ReadIterateBigValue | 扫描 500×1MB(惰性) | ~110µs | **3**(不再逐条拷贝 MB) |

读路径扫描**零分配**(分块 arena),大 value 按需解析而非提前拷贝。

## 测试

五大核心链路——读、写、压缩、重启恢复、vlog GC——均有端到端测试,且全量通过 `-race`:

- `TestFlushReadIntegrity` — flush 后 10 万 key 读回:0 丢失
- `TestDBAutoCompaction` — 6 万写入+更新+删除 → 压缩 → 全量读回,含压缩期间并发读
- `TestValueLogGCFull` — vlog GC 全链路:统计 → 挑选 → 重写 → 删旧文件 → 重开
- `TestCompactionL0ToL0 / L0ToLmax / TestLastLevelCompaction` — 三条压缩路径
- `TestCrashRecovery` — 模拟断电后的 WAL 重放

```bash
go test ./... -race -count=1          # 全量测试 + 竞争检测
go test ./benchmk/ -bench=. -benchmem -benchtime=2s -count=3   # 基准
```

## 文档

- `docs/improvements-summary.md` — 优化与修 bug 战役的通俗版总结
- `docs/improvements-detail.md` — 每项改动的代码级细节
- `docs/todo-list.md` — 按优先级排列的待办(崩溃恢复深场景、soak 测试、剩余性能项)
- `docs/write-path-optimization.md` — 写路径优化原始报告

## License

MIT — 见 [LICENSE](LICENSE)。
