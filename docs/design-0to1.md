# TrainKV 从 0 到 1:LSM-Tree 存储引擎设计文档

> 本文档基于 TrainKV 仓库(`/win_projects/goland_project/TrainKV`,纯 Go、无 CGO、约 1.5 万行源码)梳理而成。它不是代码复述,而是按"从 0 到 1 把引擎一步步造出来"的顺序,讲解每个组件的**设计动机、数据结构、磁盘/内存格式、核心算法、并发与崩溃一致性**,并给出对应的源码坐标,方便对照阅读。

**代码导航约定**:文中以 `包/文件::函数名` 形式标注源码位置,例如 `lsm/compact.go::subCompact` 表示 `lsm/compact.go` 文件中的 `subCompact` 函数。数据文件全部落在同一个工作目录(不分子目录)。

---

## 目录

- [第 0 章 系统全景与总体架构](#第-0-章-系统全景与总体架构)
- [第 1 步 领域模型与公共设施](#第-1-步-领域模型与公共设施)
- [第 2 步 固定容量 Arena 与无锁跳表](#第-2-步-固定容量-arena-与无锁跳表)
- [第 3 步 mmap 文件抽象](#第-3-步-mmap-文件抽象)
- [第 4 步 WAL 前置日志](#第-4-步-wal-前置日志)
- [第 5 步 内存表与 Flush 机制](#第-5-步-内存表与-flush-机制)
- [第 6 步 SSTable 磁盘格式与构建](#第-6-步-sstable-磁盘格式与构建)
- [第 7 步 点查路径与层间查找](#第-7-步-点查路径与层间查找)
- [第 8 步 块缓存 16 分片 W-TinyLFU](#第-8-步-块缓存-16-分片-w-tinylfu)
- [第 9 步 层级管理与 MANIFEST](#第-9-步-层级管理与-manifest)
- [第 10 步 迭代器体系与合并读](#第-10-步-迭代器体系与合并读)
- [第 11 步 Compaction 分层合并与版本回收](#第-11-步-compaction-分层合并与版本回收)
- [第 12 步 事务与 MVCC](#第-12-步-事务与-mvcc)
- [第 13 步 写通道与 Group Commit](#第-13-步-写通道与-group-commit)
- [第 14 步 Value Log 大 Value 分离与 GC](#第-14-步-value-log-大-value-分离与-gc)
- [第 15 步 启动恢复与崩溃安全](#第-15-步-启动恢复与崩溃安全)
- [第 16 步 从 0 到 1 实施路线图](#第-16-步-从-0-到-1-实施路线图)
- [附录 A 默认参数表](#附录-a-默认参数表)
- [附录 B 磁盘格式速查](#附录-b-磁盘格式速查)
- [附录 C 并发与锁使用速查](#附录-c-并发与锁使用速查)
- [附录 D 术语表](#附录-d-术语表)
- [附录 E 常见设计问题 FAQ](#附录-e-常见设计问题-faq)
- [附录 F 本仓库配套资料](#附录-f-本仓库配套资料)

# 第 0 章 系统全景与总体架构

## 0.1 一句话概括

TrainKV 是一个**写优化的 LSM-Tree 嵌入式 KV 存储**,采用 **KV 分离(value 日志)** 架构:小于阈值的 value 随 key 一起存进 LSM(内存跳表 + WAL → SSTable 分层),大于等于阈值的 value 单独追加进 value log(下文称 vlog),LSM 里只留 12 字节指针。上层提供**基于时间戳的 MVCC 读写事务**与 group commit 批量写。

整体设计约 1.5 万行,无 CGO,依赖 protobuf(gogo 生成 `pb` 包)、`gofrs/flock`(目录锁)、`cespare/xxhash/v2`、`golang.org/x/sys/unix`(mmap)。

## 0.2 三层架构图

```
┌──────────────────────────────────────────────────────────────────┐
│                         应用 API(TrainKV 包)                       │
│   TrainKV.Open / NewTransaction(update) / txn.Set/Get/Delete/     │
│   Commit / Discard / Update / View / NewIterator / RunValueLogGC  │
├──────────────────────────────────────────────────────────────────┤
│  事务层 txn.go          TransactionManager(ts 分配/LimitMark×2)    │
│                        startMark 水位 → 压缩安全回收版本            │
│                        commitMark 水位 → 新事务看到全部已提交       │
├──────────────────────────────────────────────────────────────────┤
│  写通道 db.go           writeCh(1000) → handleWriteCh 攒批          │
│                        → WriteRequest:vlog 先行 → LSM(WAL+跳表)    │
├───────────────┬──────────────────────────────┬───────────────────┤
│  LSM 组件 lsm/ │  ValueLog vlog.go/vlogFile.go│  块缓存 utils/cache │
│  memTable      │  追加式大 value 文件          │  16 分片 W-TinyLFU │
│  (skl 跳表+WAL) │  GC:compaction discard 统计 │  (wlru+slru+door+  │
│  flush→L0      │  → 重写有效记录 → 删旧文件    │   cmSketch)        │
│  compact L0..L6│  读: mmap 零拷贝+读锁回调     │  key=fid<<32|blk   │
│  MANIFEST      │                               │                   │
└───────────────┴──────────────────────────────┴───────────────────┘
  基础设施:file/mmap(预分配+mmap+msync) · utils(bloom/hash/closer/
           throttle/limiterMark/chunkedArena/crc32) · model(Entry 等)
```

组件职责速查表:

| 组件 | 位置 | 职责 |
|---|---|---|
| `skl` | `skl/` | 固定容量 arena + 无锁跳表,作为内存表的存储结构 |
| `lsm.LSM` | `lsm/lsm.go` | 内存表(可写 1 个)+ 不可变内存表队列 + 层级管理器 + flush 协程 |
| `lsm.WAL` | `lsm/wal.go` | 内存表的前置日志(mmap 直写),崩溃后重放 |
| `lsm.SstBuilder/Table` | `lsm/sstBuilder.go`、`table.go` | SST 构建端与读取端(块索引、布隆、块缓存接入) |
| `lsm.LevelsManger` | `lsm/levelsManger.go` | 7 层 LevelHandler、flush 入口、fid 分配、水位接收 |
| `lsm.LevelHandler` | `lsm/levelHandler.go` | 单层表列表、排序、层内查找、增删表 |
| `lsm.ManifestFile` | `lsm/manifest.go` | 表注册元数据(追加式清单,支持重写) |
| `lsm/compact.go` | `lsm/compact.go` | 压缩调度、选表、并行 sub-compaction、版本回收 |
| `lsm/lsm_iterator.go` | `lsm/lsm_iterator.go` | block/table/concat/merging 四级迭代器 |
| `utils/cache` | `utils/cache/` | W-TinyLFU 缓存(win-LRU + SLRU + 门卫 bloom + CM-Sketch) |
| `ValueLog` | `vlog.go`、`vlogFile.go` | 大 value 追加日志 + discard 统计驱动 GC |
| `txn.go` | `txn.go` | MVCC 事务:startTs/commitTs、冲突检测、读己之写 |
| `db.go` | `db.go` | TrainKV 门面:Open/Close、写通道、batch 写、vlog GC 入口 |
| `model/common/utils` | `model/`、`common/`、`utils/` | Entry/Key/ValuePtr/Request、常量错误、工具 |
| `file/mmap` | `file/`、`mmap/` | mmap 文件生命周期与平台差异封装 |

## 0.3 工作目录里最终会出现哪些文件

| 文件 | 说明 |
|---|---|
| `LOCKFILE` | flock 目录锁,防多进程同时打开 |
| `00001.wal`…`0000N.wal` | WAL(每个内存表一个,`%05d.wal`) |
| `00001.sst`…`0000N.sst` | SSTable(`%05d.sst`) |
| `00001.vlog`…`0000N.vlog` | value log(`%05d.vlog`) |
| `MANIFEST` / `REWRITEMANIFEST` | 表注册清单(追加式 + 重写) |
| 特别说明:`wal` 与 `sst` **共享同一个 fid 命名空间**(内存表 fid 由 LSM 分配,flush 后同名 sst),而 **vlog 的 fid 与 LSM 完全无关**(vlog 自己从 1 开始分配,见 `vlog.go::createVlogFile` 注释) ||

## 0.4 贯穿全文的核心概念

1. **内部 key = userKey ‖ 8B ts(取反存储)**。ts 越大,取反后字节序越小,于是 `bytes.Compare` 下**同一 userKey 的新版本排在最前**。这是全系统排序、合并、可见性判断的基石(`model/Key.go::KeyWithTs`)。
2. **Entry(条目)** 是读写的最小单元:`Key(带 ts)`、`Value`、`ExpiresAt`、`Meta`(4 个位标记:删除/值指针/事务普通条目/事务结束标记)。
3. **版本(version/ts)** 语义:普通写事务提交时获得单调递增的 commitTs,写入 key 尾部;读快照 = startTs,任何层只返回 `version ≤ startTs` 的最新条目。删除是一条 `BitDelete` 墓碑记录,也占一个版本。
4. **水位(watermark)** 是"后台清理能否动手"的唯一裁判:两个 `LimitMark` 分别跟踪"读事务结束水位(startMark→discardTs)"与"提交完成水位(commitMark)",压缩据此丢弃旧版本,事务据此保证快照完整性。
5. **引用计数**贯穿表/内存表/跳表的生命周期:压缩、读、迭代都可以安全持引用,归零才真正删文件/清缓存。

## 0.5 设计决策总表(带着这些问题读下文)

| # | 决策 | 理由 | 出处 |
|---|---|---|---|
| 1 | 跳表 + arena,而非 B 树/红黑树 | 写内存友好、无锁并发实现简单、天然有序;arena 消除每 key 多次小分配 | 第 2 步 |
| 2 | 全部日志/表文件走 mmap | 内核页缓存即缓冲区,少一次用户态拷贝;崩溃时由 msync 控制持久化边界 | 第 3 步 |
| 3 | 大 value 分离到 vlog | 压 memtable 内存/SST 大小,缓解压缩写放大;大 value 只需追加写 | 第 14 步 |
| 4 | 先写 vlog 再写 WAL/memtable | 保证 WAL 里指向 vlog 的指针永远能读到完整 value | 第 13/14/15 步 |
| 5 | 事务条目在 WAL 中以 `BitTxn…BitFinTxn` 分组 | 重放时组内原子生效,提交"要么全部恢复要么全部丢弃" | 第 4/5 步 |
| 6 | L0 层按 maxVersion 降序、L1+ 按 minKey 升序且互不重叠 | L0 内表重叠(无序),新表在前可早停;L1+ 有序可二分 | 第 9 步 |
| 7 | 版本回收水位 = 读事务结束水位(discardTs) | MVCC 下"没人会再读的版本"才能物理删除,否则破坏快照 | 第 12 步 |
| 8 | 块缓存用 W-TinyLFU 而非 LRU | 扫描型访问不会冲垮热数据;频次(CM-Sketch)而非仅新近度 | 第 8 步 |
| 9 | 压缩不经过块缓存(IsSetCache=false) | 压缩全量顺序读,喂缓存是纯污染 | 第 6/7/11 步 |
| 10 | 单写协程串行化落盘 + 有界通道攒批 | 免锁、天然 group commit、可背压 | 第 13 步 |
| 11 | 内存表轮转以 arena 实际用量(非 WAL 大小)为准 | 固定容量 arena 不可扩容,必须先判上界再写,否则越界 | 第 2/5 步 |
| 12 | 固定容量 arena(不复用、不迁移) | 已返回切片永久有效,迭代器/读路径零拷贝安全 | 第 2 步 |

---

# 第 1 步 领域模型与公共设施

> 目标:先把"一条数据长什么样、怎么比大小、怎么落盘编解码、怎么在进程内传递"定义清楚。所有上层组件的复杂度都建立在这组模型上。

## 1.1 内部 Key 编码:userKey ‖ 8B 取反 ts

磁盘/内存中流转的 key 一律是 `userKey + 8B ts`,ts 按 `math.MaxUint64 - ts` 大端写入(`model/Key.go::KeyWithTs`)。例如 userKey=`"foo"`,ts=5 → `"foo" + MaxUint64-5` 的 8 字节大端。

为什么取反:

- `bytes.Compare` 直接可用:同一 userKey 的多版本中,**大 ts 的编码字节序反而更小**,升序遍历天然得到"新版本在前";
- 无需自定义"先比 key 再比版本降序"的比较器,`bytes.Compare` 一次搞定;
- 布隆过滤器建在**去掉 ts 的 userKey** 上,前缀相同的多版本共享过滤位(第 7 步)。

配套函数(`model/Key.go`):

- `ParseTsVersion(key)`:`MaxUint64 - BigEndian(尾 8B)`,解析出版本号;
- `ParseKey(key)`:`key[:len-8]`,剥掉版本;
- `SameKeyNoTs(a,b)`:`len` 相等且剥版本后字节相等(注意不同 userKey 长度不同直接 false);
- `CompareKeyWithTs(k1,k2)`:**先比 userKey 再比 ts**,处理 userKey 长度不同的前缀情形——长度相同直接全量 `bytes.Compare`(首个不同字节要么落在 userKey 区要么落在 ts 区,语义一致);长度不同则先比公共前缀,前缀短者小,再比 raw 长度;
- `KeyWithTsPooled`/`KeyTsBufPool`:提交路径取 `key+8` 缓冲池(指针装箱零分配),用后须归还;
- `SafeCopy`:无条件新分配拷贝(注意忽略 dst,总是 make 新内存)。

## 1.2 Entry:内存/写路径数据模型

```go
type Entry struct {
    Key, Value []byte    // Key 带 8B ts
    ExpiresAt  uint64    // 过期时间(Unix 秒),0=永不过期
    Meta       byte      // 位标记,见下
    Version    uint64    // 事务提交 ts(读路径显式解析)
    HeaderLen int; Offset uint32; ValThreshold int64 // 运行期瞬态
}
```

Meta 位掩码(`common/const.go`):

| 位 | 常量 | 含义 |
|---|---|---|
| 1<<0 | `BitDelete` | 删除墓碑(无 value) |
| 1<<2 | `BitValuePointer` | Value 是 12B vlog 指针(大 value 分离) |
| 1<<3 | `BitTxn` | 普通事务条目(提交组内,WAL 重放时需组原子性, 不进vlog) |
| 1<<4 | `BitFinTxn` | 事务结束标记条目(只在 WAL 出现,不进跳表) |

Entry 生命周期:事务新建的 Entry 走 `EntryPool`,`Discard` 时 `Release()` 归还并清引用,防池化泄漏。

`EstimateSize(valThreshold)`:估算一条 Entry 最终占用 LSM 的空间,小 value(内联)按 `len(key)+len(value)+1`;大 value(指针)按 `len(key)+12+1`。用于事务 batch 限额与写通道校验。

## 1.3 ValueExt:跳表/SST 块内的 value 编码

SST 块内与 arena 内,value 的紧凑编码是:`1B meta + uvarint(ExpiresAt) + value 字节`(`model/ValExt.go::EncodeVal`)。`Version` 不落盘,读取时从 key 尾部解析。解码为 `ValueExt{Meta, Value, ExpiresAt, Version}`。

`EncodeValSize = 1 + sizeVarint(ExpiresAt) + len(value)`。

## 1.4 EntryHeader:WAL/vlog 的记录头

`model/Entry.go::EntryHeader`,编码为 `1B meta + uvarint(KLen) + uvarint(VLen) + uvarint(ExpiresAt)`(KLen/VLen 常见 4~5B、ExpiresAt 8~10B,总长上限 `MaxHeaderSize = 21`)。配套 `HashReader`(包装 reader 边读边喂 crc32,Castagnoli 表)让 WAL/vlog 顺序解码每条目复用同一 crc 对象。

## 1.5 ValuePtr:12 字节的 vlog 指针

```go
type ValuePtr struct { Len, Offset, Fid uint32 }   // 共 12B
```

`Encode/Decode` 直接以 unsafe 按 12B 原样拷贝(依赖结构体内存布局,原生字节序,不做跨端移植)。`model.IsValPtr(entry)` 判断 `Meta&BitValuePointer`。`IsDiscardEntry(vlogEntry, lsmEntry)` 是 vlog GC 的核心判定(第 14 步)。`IsDeletedOrExpired(meta, expiresAt)` 判断墓碑/已过期。

数值转换:`BytesToU32/U32ToBytes/U64ToBytes` 为大端显式转换(文件尾标量);`U32SliceToBytes/BytesToU32Slice` 用 reflect.SliceHeader 做 **u32 切片与字节零拷贝互转**(块内 entryOffsets 数组专用,原生序)。

## 1.6 Request:写通道的批量载体

```go
type Request struct {
    Entries []*Entry
    ValPtr  []ValuePtr   // 与 Entries 对齐;LSM 直写条目以零值占位
    Wg      sync.WaitGroup
    Err     error
    ref     int32
    valPtrScratch []byte // 12B 复用缓冲,EncodeValPtr 编码用
}
```

- 池化(`RequestPool`)+ 原子 `IncrRef/DecrRef`,归零才还池;
- `Wg + Err`:提交方 `Wait()` 阻塞至落盘完成(或失败),这是 txn.Commit 的返回语义来源;
- `SendToWriteCh` 在入队前校验 `MaxBatchCount/MaxBatchSize`,超限返回 `ErrBatchTooLarge`。

## 1.7 迭代器契约(interfaces)

`interfaces.Iterator`:`Name/Next/Valid/Rewind/Item/Seek/Close`,实现者有:跳表迭代器、blockIterator、TableIterator、ConcatIterator、MergingIterator、txn 的 pendingWritesIterator、TxnIterator。`Item{Item model.Entry; VP model.ValuePtr; Vlog model.ValueReader}` 支持**大 value 惰性读**:`Item.Value()` 在 `VP.Len>0 && Vlog!=nil` 时经 `vlog.Read(vp)` 取数,**并必须调用返回的 callBack 释放文件读锁**(vlog 文件可能被 GC 轮转删除,读锁回调是安全屏障,第 14 步展开)。`Options{Prefix, IsAsc, IsSetCache}` 控制方向与是否喂块缓存。

## 1.8 公共工具速览(utils/common)

- **哈希**:`utils/bloom.go` 用 leveldb 同款 32 位混合哈希(`seed=0xbc9f1d34,m=0xc6a4a793`)为 SST 建 bloom;`utils/hash.go` 用 `//go:linkname` 引入 `runtime.memhash`(Go map 同款)+ `xxhash.Sum64`,组成双哈希给事务冲突检测/缓存分片用。
- **crc32**:全局共享 Castagnoli 表 `common.CastigationCryTable`,WAL/vlog/SST 块/索引全部用它。
- **错误与断言**:`common/errors.go` 定义 `ErrKeyNotFound/ErrBlockEOF/ErrWalInvalidCrc/ErrConflict…`;`CondPanicf` 是热路径防御断言(避免每次 fmt.Errorf 分配)。
- **Closer**:`close(信号) 先于 WaitGroup.Wait` 的多协程优雅停机骨架(顺序是关键)。
- **Throttle**:最大并发信号量 + 首错广播 + Finish 汇合(压缩子任务用)。
- **ChunkedArena**:64KB 分块、无释放、迭代器 Item key 稳定副本的来源。
- **FastRand**、`FileOptions`(fid/文件名/大小统一传参)、`SyncDir`(目录 fsync;Windows 下为 no-op)。

> 第 1 步的验证:写单测覆盖 KeyWithTs/ParseTsVersion 往返、CompareKeyWithTs 的三种情形(同长/前缀/不同 key)、EntryHeader 编解码与 crc、ValuePtr unsafe 往返。这些是后面所有排序正确性的地基。

# 第 2 步 固定容量 Arena 与无锁跳表

> 目标:做出内存表的核心结构——一个支持**并发写、并发读、有序迭代、零垃圾分配**的 KV 容器。TrainKV 选择"固定容量 arena + 无锁跳表"(Badger/rsms 的经典组合)。

## 2.1 设计取舍

- **为什么跳表**:内存表只需追加写 + 有序读;跳表实现简单,且天然支持无锁并发(只改指针,可 CAS);B 树/红黑树的平衡操作无法在无锁下简单落地。
- **为什么 arena 而非直接堆分配节点**:每写一条 key 就分配 2~3 个对象(节点/key/value),arena 一次分配一大块,后续按偏移切,把每 key 多次小分配摊销成 O(1) 的大块分配,极大缓解 GC 压力。
- **为什么节点里存 arena 偏移(offset)而不是 Go 指针**:Go GC 无法跟踪"指针藏在 uint32 里",存 offset 等于告诉 GC"这只是一个数字",arena 一整块只被外部引用一次;同时 32 位偏移把节点/键/值定位压缩成 4 字节,`skipNode` 里 `next [20]uint32` 只有 80B。
- **为什么固定容量且不复用**:arena 一经分配不移动不释放,已返回的切片永久有效 → 跳表迭代器可以零拷贝直接引用 key/value 视图,读写并发时写方不会让读方悬垂(写只追加,不迁移)。代价是容量写死,写满必须轮转——因此**调用方(memtable 轮转)必须用上界估算保证写前不越界**(见 2.7)。

## 2.2 Arena 布局与分配(skl/arena.go)

```go
type Arena struct { data []byte; sizes uint32 }  // sizes = 已分配字节(从 1 起,0 保留作"空偏移")
```

- `allocate(sz)`:`sizes` 原子加 sz,返回旧值作为偏移;`AssertTrue` 保证不越界(固定容量,越界=内部 bug)。
- `AllocateNode(height)`:按 `nodeAlign`(7)对齐返回偏移(节点可能未用满 `next[maxHeight]`,按实际 height 少分配)。
- `PutKey/PutVal` 拷入并返回偏移;`getNode(offset)` 用 unsafe 把 `&data[offset]` 转成 `*skipNode`。
- 对齐的意义:保证 `skipNode` 字段不跨 8B 边界,后续 64 位原子读写安全。

## 2.3 节点与跳表结构(skl/skipList.go)

```go
type skipNode struct {
    value    uint64    // 高 32 位=valSize,低 32 位=valOffset(在 arena 中)
    keyOffset uint32; keySize uint32
    height   uint16
    next     [maxHeight]uint32
}
type SkipList struct {
    arena *Arena; num int64; headOffset uint32
    height int32                 // 当前最高层(原子)
    ref atomic.Int32; OnClose func()
}
```

- 常量:`maxHeight = 20`;`heightIncrease = MaxUint32/3` → `randomHeight()` 每次掷 1/3 概率升层,平均高度 ≈ 1/(1-1/3)=1.5,最高 20。
- value 打包 `valSize<<32|valOffset`:读取用 `atomic.LoadUint64` 一次拿全(见 2.4 的原地更新场景)。
- `skipNode.value`/`next[]` 更新全部走原子操作:`casNextOffset` 层内 CAS、`setVal` 原子写 value——写写并发安全的来源。

## 2.4 Put(并发插入,核心算法)

`SkipList::Put`(与 Badger 的 lock-free skiplist 同源)步骤:

1. 从当前高度向下逐层 `findSpliceForLevel(key, prev[i+1], i)`:在**层 i** 上找插入位置 `prev[i]/next[i]`(跳过所有 key < 目标);若找到 `cmp==0`(同 keyTs,即原地更新),`PutVal` 新 value → `setVal` 原子覆盖后返回;
2. 抛硬币得 `height = randomHeight()`;
3. 若 `height > listHeight`:`CAS(height)` 竞争抬升全局高度(失败则重读);
4. **逐层插入**:先把 `x.next[i] = next[i]`,再 `casNextOffset(i, next[i], x)`;CAS 失败说明有并发写者抢先插入了,重新 `findSpliceForLevel` 后再试;层 0 CAS 成功才 `num++`;期间若发现 `prev[i]==next[i]`(被并发者写成了同 key),走"原地更新"分支覆盖 value。

要点:节点分配(`newSkipNode`,arena 分配是原子的)先于 CAS;同一 key 的并发写最终只有一个节点胜出,输家变成"覆盖 value"——因为 `value` 字段是原子 64 位,读方永远看到完整的新/旧 value 之一。

## 2.5 Get:找到"最接近且 ≥ key"的节点

`findNear(key, less, allowEqual)` 从顶向下逼近:每层跳过 `< key` 的节点;到底层返回第一个 `≥ key`(或 `≤`,取决于 less 参数)的节点。`Get(userKeyTs)` 用 `(less=false, allowEqual=true)` 找到近邻后:

1. `SameKeyNoTs` 校验 userKey 相同,否则返回 `ValueExt{Version:0}`(约定的"未找到"哨兵);
2. 命中后**显式 `ParseTsVersion(nearKeyTs)` 解析版本号**填进返回值——近邻节点可能携带更大(更新)的版本,调用方(LSM.Get)需要真实版本号做可见性判断(第 7 步)。这是"跳表内多个版本共存"时的正确性关键。

注意:跳表**每 (userKey,ts) 唯一**,但同一 userKey 的不同版本是不同节点,按 ts 取反排序后新版本靠前。

## 2.6 迭代器与引用计数生命周期

- `NewSkipListIterator` 先 `IncrRef`,`Close` 时 `DecrRef`;`IncrRef/DecrRef` 原子计数,**归零才触发 `OnClose`**(由 memTable 设置为"关闭并删除 WAL",`lsm/memTable.go::NewMemoryTable`)并把 arena 置空。读路径(LSM.Get)对每个内存表先 IncrRef 再操作,保证迭代/flush 期间表不被销毁。
- 迭代器直接持 `curr *skipNode`,`Next` 走 level0;Key/Value 都是 arena 零拷贝视图(arena 只追加,视图稳定)。

## 2.7 内存上限与轮转预判(重要的不变量)

arena **容量固定不可扩容**,所以 memtable 必须在写之前判断会不会写爆:

- `EstimateEntryMemSize(e)`:单条 entry 占用上界 ≈ `MaxSkipNodeSize+nodeAlign`(最大高度节点)+ `len(key)` + `len(value)+11`(ValueExt 编码余量);
- `lsm.LSM::Put`:`MemBytes()(arena 已用)+ 估算 > MemTableSize` → 先 `Rotate()` 换新表再写,保证写入后绝不越界(注释明确:旧逻辑按 WAL 大小轮转无法约束 arena 用量)。

> 验证:跳表单测覆盖插入/查找/删除覆盖/多版本排序/原地更新;`-race` 下多协程 Put+Get+迭代。

---

# 第 3 步 mmap 文件抽象

> 目标:给 WAL、vlog、SST 三种文件一个统一的"打开-写-同步-截断-关闭"模型。核心思想:**预分配文件大小 → mmap → 直接写映射区 → 只对新增脏区间 msync → 截断收尾**。

## 3.1 为什么统一 mmap

1. 省一次用户态拷贝:写入直接编码进 `Buf[]`;读取直接拿映射区子切片(零拷贝);
2. 页缓存由内核管理,不需要自建读缓冲;
3. 预分配 + 稀疏页:文件先 truncate 到目标大小再映射,mmap 后未写页不占实际磁盘(稀疏),写满哪页才落哪页;轮转/收尾时再 truncate 到真实大小;
4. `msync(MS_SYNC)` 给出精确的持久化边界,崩溃一致性可控。

## 3.2 MmapFile 生命周期(file/mmapFile.go)

```go
type MmapFile struct { Fd *os.File; Buf []byte; BufLen int64 }
```

| 操作 | 语义 |
|---|---|
| `OpenMmapFile(name, flag, maxSz)` | 打开;**新文件(size==0 && maxSz>0)先 Fd.Truncate(maxSz) 再 mmap**(writable 由 flag 非 O_RDONLY 判定);size==0 时补一次目录 SyncDir |
| `Read(b, off)` | 拷贝语义,越界返回 io.EOF |
| `Bytes(off, sz)` | **零拷贝子切片**,越界 io.EOF |
| `Sync()` | 整区 `Msync(MS_SYNC)` |
| `SyncRange(n)` | 只同步 `[0, n)`(vlog 截断前用,避免遍历大片干净页) |
| `SyncDirtyRange(off, n)` | 只同步 `[off,n)` 脏页;`start = off &^ (pagesize-1)` **向下页对齐**(msync 要求页对齐),调用方保证 `[0,off)` 上次已同步——WAL/vlog 每次提交只同步新增区间的关键 |
| `AppendBuffer(offset, buf)` | 指数扩容后拷贝(1GB 封顶);全项目实际无人调用,写路径都是"直接编进 Buf + 必要时 Truncate" |
| `Truncate(maxSz)` | **先整区 Sync 再截断再重映射**(防止截断丢脏数据)。linux:`ftruncate + mremap(MREMAP_MAYMOVE)` 原地扩容;非 linux:unmap → ftruncate → 重新 mmap |
| `Close()` | Sync → Unmap → Close |
| `Delete()` | unmap → Truncate(0) → Close → os.Remove |

## 3.3 平台差异(mmap/*.go)

| 能力 | linux | darwin | windows |
|---|---|---|---|
| mmap | `unix.Mmap(MAP_SHARED)`,无 MAP_POPULATE(按需缺页,配合稀疏预分配) | 同 linux | 先补 Truncate 到 size(CreateFileMapping+MapViewOfFile) |
| 扩容 | `SYS_MREMAP + MREMAP_MAYMOVE`,用 reflect.SliceHeader 改写 Data/Cap/Len(unsafe 高危区) | 不支持 → 走"unmap+重映射"路径(mmapFile_defalut.go) | 不支持 → 同上 |
| madvise | MADV_NORMAL / MADV_RANDOM / MADV_SEQUENTIAL 全支持 | 裸 syscall 实现,remap 是 stub | 两个 madvise 均为空操作 |
| msync | `Msync(MS_SYNC)` 阻塞式 | 同左 | FlushViewOfFile |
| SyncDir | 真目录 fsync | 真目录 fsync | no-op(Badger#699:Windows 目录无法 fsync) |

两个易踩坑的细节(代码注释原话):

- `mmap_unmmap` 先校验 `len==cap` 再 munmap,防止误 munmap 映射区的子切片(EINVAL);
- 三平台能力阶梯:linux 独享 mremap 原地扩容与 MADV_SEQUENTIAL,vlog 打开时调用 `MadviseSequential`(顺序读提示,失败仅打日志不致命);
- vlog 的"unmap→重映射"期间必须**持文件写锁**(`VLogFile.Lock`),否则并发读方会访问已 unmapped 的内存。

> 验证:crash 测试的底座——mmap 写 + SyncRange/SyncDirtyRange 边界、Truncate 后数据保持、Delete 后路径不存在。

---

# 第 4 步 WAL 前置日志

> 目标:写进跳表的数据,先以追加日志形式落盘,保证进程崩溃后内存表可完整重建。WAL 也是事务原子性的物理载体。

## 4.1 记录格式(与 vlog 相同)

```
+-------------------------------+-----+-------+--------+
| EntryHeader: meta|klen|vlen|  | key | value | crc32  |
| ExpiresAt(varint, 1+3×varint) |     |       | 4B BE  |
+-------------------------------+-----+-------+--------+
```

crc32(Castagnoli)覆盖 header+key+value 整体。

## 4.2 写入(EncodeAt:直写 mmap)

`WAL.Write(e)` → `EncodeAt(e, writeAt)`:把 header 编码进栈上数组,key/value `copy` 进 `file.Buf[offset:]`,对已写区段就地算 crc(数据刚写入仍在 CPU 缓存)。预分配(MemTableSize)不够才 `Truncate`(正常轮转预判下不触发)。写路径被 handleWriteCh 串行化,`writeAt` 原子递增,无需锁。

## 4.3 增量同步(SyncFile)

WAL 预截断到 MemTableSize,整区 msync 会随文件增长线性变慢 → 只同步 `[lastSynced, Size)`(`SyncDirtyRange`)。`lastSynced` 只推进不回退。

## 4.4 恢复读取:容忍"半条记录"

`WalDecode` + `WAL.Read` 的容错规则:

| 错误 | 处理 |
|---|---|
| EOF / ErrUnexpectedEOF / ErrTruncate | 文件尾部半条记录 → 视为正常结束 |
| ErrWalInvalidCrc 且**其后全为零填充** | 崩溃时最后一条未完整落盘的记录 → 视为尾部,正常结束 |
| 其他(文件中间损坏) | panic(真实损坏,拒绝静默丢数据) |

恢复方(`recovery2SkipList`,第 5 步)会拿到"最后一个完整有效记录的结束偏移"并 `Truncate` 掉尾部垃圾。

## 4.5 与事务原子性的关系(先铺垫)

普通事务提交的一批条目以 `BitTxn` 开头(逐条携带),以一条 `BitFinTxn` 结束标记条目收尾(fin 条目 value 是 commitTs 的十进制字符串,key 是固定 `!train!txn!fin`)。WAL 重放要求:

- `BitTxn` 条目必须**连续且同 ts**,中间不得插入非本组条目(否则 `ErrBadTxn`);
- 只有看到配套 `BitFinTxn`(且其携带 ts 一致)才把整组条目写入跳表——**组内原子提交**;
- 非事务条目(vlog GC 重写、discard 统计)不分组,单条即提交,但**不得出现在某事务组中间**(同样报 ErrBadTxn)。

这样设计是因为事务的原子性无法靠"单条 crc"表达:提交过程可能在中途崩溃,WAL 里留下半个组;恢复时以 fin 标记为界,不完整的组整体丢弃,事务不可能部分生效。

> 验证:`crash_test.go` 在提交窗口反复 kill -9 后重启,数据要么全部可见要么全部不可见。

---

# 第 5 步 内存表与 Flush 机制

> 目标:把"跳表 + WAL"组装成 MemoryTable,定义轮转(immutable 化)、flush 落盘、崩溃重放,以及三者与 LSM 查找路径的衔接。

## 5.1 MemoryTable 结构

```go
type MemoryTable struct {
    lsm *LSM
    skipList   *skl.SkipList
    wal        *WAL
    maxVersion uint64   // 本表内最大版本,实时维护
    name       string   // "fid.memtable"
}
```

`lsm.NewMemoryTable()`:`NextFileID()` 拿 fid(与 sst 共享命名空间)→ `NewSkipList(MemTableSize)` → `OpenWalFile(%05d.wal)`,并把跳表 `OnClose` 钩子设为"关闭并删除 WAL"。

`Put(e)` = **先 `wal.Write(e)` 再 `skipList.put(e)`**;`skipList.put` 写跳表并维护 `maxVersion`。恢复(重放)路径与在线写路径共用 `put`,保证 maxVersion 语义一致。`BitFinTxn` 条目只进 WAL 不进跳表(`Put` 里特判)。

## 5.2 轮转(immutable 化 + 有界队列背压)

`lsm.Rotate()`:

1. `Lock`;若当前表为空直接返回(避免空表入队);
2. `immemoryTables = append(imm, 旧表)`;`memoryTable = NewMemoryTable()`(新 fid+新 WAL);
3. `SyncWrites` 模式下**先把旧表 WAL 同步落盘**(当前批次可能已跨轮转写入旧 WAL,兑现"提交返回=已持久化");
4. `flushMemTable <- 旧表`:向**容量 = WaitFlushMemTables(默认 6)的有界通道**投递 → 通道满时 Put 侧阻塞,形成自然背压,内存不会无限堆积。

flush 消费方是 `StartFlushMemTable` **单协程**:取表 → `LevelManger.flush(im)` 落 SST(第 6 步)→ 从 `immemoryTables` 头部摘除并 `skipList.DecrRef()`(跳表归零时自动删 WAL)。收到关闭信号时排空通道内剩余表再退出。

注意:immutable 列表顺序即"新→旧"?——`Rotate` 是 append 尾部,**列表尾部最新**;而查找路径(见第 7 步)会倒序遍历,先查最新表。flush 协程则从**头部(最老)**开始处理,`flushMemTable` 通道 FIFO 保证顺序。

## 5.3 Get:内存侧查找

`MemoryTable.Get(keyTs)`:IncrRef 保护 → 跳表 Get → 命中则把 ValueExt 组装成 Entry 返回(Version 从 key 显式解析);未命中返回 `Version:0` 哨兵。删除墓碑(有版本、无 value)也是"命中"。

## 5.4 崩溃重放(lsm.recovery)

启动时(`lsm.go::recovery`):

1. 扫目录收集所有 `*.wal` 的 fid,升序排序;
2. `sstMaxID := LevelManger.maxFID`(manifest 中已登记的最大表 id)。**若某个 wal fid == sstMaxID,直接删该 wal**——这是"flush 已把 5.wal 转成 5.sst 并写入 manifest,但还没来得及删 wal 就宕机"的窗口:重启时 5.sst 已从 manifest 恢复,残留的 5.wal 必须丢弃(否则 fid 撞车且数据重复);
3. 其余每个 wal:`openMemTable` → `recovery2SkipList` 重放 → 空表直接丢弃;有数据者成为 immutable 表并重新入 flush 队列;
4. `maxFID = max(maxFID, 最大 wal fid)`(只有 wal 没有 sst 的表会在本次启动补 flush);
5. 最后返回 `NewMemoryTable()` 作为新的可写表。

### recovery2SkipList:重放状态机(lsm/memTable.go)

```go
lastCommitTs = 0; entries = []
for 逐条读 WAL:
  BitTxn 条目:  要求组连续(ts 必须 == lastCommitTs,首条时记录)→ 暂存 entries
  BitFinTxn 条目: 要求 value(十进制 ts) == lastCommitTs → 组完整:
                 validEndOffset = 当前位置;整组 put 进跳表;清空暂存
  普通条目:      要求当前不在组中(lastCommitTs==0),单条 put,更新 validEndOffset
  任何违例(组被打断/ts 不一致):return ErrBadTxn(损坏)
```

结束后 `walFile.Truncate(validEndOffset)` 裁掉未提交的尾部半组。于是:提交成功的组全部恢复;提交中的组整体丢弃——对应第 4.5 节的原子性承诺。注意**普通条目的 put 立即生效**,因为它是单条原子(GC 重写/统计),而事务组的 put 延迟到 fin 之后。

## 5.5 LSM 结构回顾(lsm/lsm.go)

```go
type LSM struct {
    sync.RWMutex              // 保护 memoryTable 指针与 immemoryTables 切片
    memoryTable *MemoryTable  // 唯一可写表
    immemoryTables []*MemoryTable
    LevelManger *LevelsManger
    flushMemTable chan *MemoryTable
    ExpiredValPtrChan …       // (预留)压缩丢弃的 vlog 指针统计通道
}
```

`Put` 入口做轮转预判(2.7)→ 写表。`SyncWalFile` 透传。`MaxVersion()` 遍历内存表与每层表(逐层加 RLock)取最大版本,供事务管理器播种水位。`Close`:关当前表与所有 immutable 的 WAL(有数据则 Truncate 收尾再 Close,空则直接 CloseAndRemove),再关 LevelManger。

> 验证:`db_test.go` 的崩溃/重启用例、`compaction_gc_test.go` 中的轮转压力、`-race` 并发 Put+Get。

# 第 6 步 SSTable 磁盘格式与构建

> 目标:把有序数据落成不可变、自描述、可随机读的文件。格式设计直接影响读放大(索引)与空间放大(前缀压缩)。

## 6.1 整体文件布局

```
+----------+----------+-----+----------+--------------------------------+
| block 0  | block 1  | ... | block N  | TableIndex(protobuf, 变长)      |
|          |          |     |          | = BlockOffset[] + BloomFilter + |
|          |          |     |          |   MaxVersion + KeyCount +       |
|          |          |     |          |   StaleDataSize                 |
+----------+----------+-----+----------+--------------------------------+
   ↑ 索引只存每个 block 的 baseKey / 文件内 offset / size(BlockOffset)
+----------------------------------+------------+-------------+-------+
| len(TableIndex) 4B BE            | checksum 8B| len(chk)4B  |       |
+----------------------------------+------------+-------------+-------+
文件尾(倒序读取):4B chkLen → checksum → 4B idxLen → 索引数据(校验)
```

- 表尾布局:`[TableIndex][4B len(TableIndex)][8B checksum(TableIndex)][4B len(checksum)]`。索引是 protobuf(`pb.TableIndex`),整体 crc32 校验,防止索引损坏导致越界读。
- `BlockOffset{Key, Offset, Size}`:每个 block 的 baseKey(块内第一条 key)、块在文件中的起始偏移、块大小(含块尾元数据)。
- `TableIndex` 还携带:**BloomFilter**(全表 userKey 集合)、**MaxVersion**(建表时见到的最大 ts)、**KeyCount**、**StaleDataSize**(压缩时标记的失效字节数,是 Lmax→Lmax 压缩的决策输入,必须持久化)。

## 6.2 Block 内部布局(核心:前缀压缩 + 重启点数组)

```
+-----------------------------------------------------------------------------------+
| entry0 | entry1 | ... | entryN | entryOffsets[](每 entry 4B, 相对块首) | num 4B | 
| (变长)  (变长)          (变长)   |  = restart point 数组                    |       |
+-----------------------------------------------------------------------------------+
| chk 8B | chkLen 4B |
+--------------------+
```

每条 entry:

```
+------------------+------------+----------------------------------+
| entryHeader 4B   | 差分 key   | value: 1B meta + varint(Exp) +  |
| overlap:u16      | (dif 字节) | value 字节                       |
| dif:u16          |            |                                  |
+------------------+------------+----------------------------------+
```

- **前缀压缩**:entry 的 key = 与块内 baseKey 的公共前缀长度 `overlap` + 剩余差异字节 `dif`。重建规则(`blockIterator.loadIndex`):`key = baseKey[0:overlap] + diffKey`;由于同块 key 前缀随索引递增,可用 `prevOverlap` 增量重建,二分探测阶段只解码 key 不拷贝(性能关键)。
- 每条 entry 的**块内起始偏移**记入 `entryOffsets[]`(就是重启点数组;与 RocksDB"定长间隔重启点"不同,TrainKV **每条都记**,换取出更简单的块内二分——entryOffsets 是 4B×N,约占块 5~10% 开销)。
- 块尾:`entryOffsets 数组 + numEntries(4B) + checksum(8B,覆盖 entries+offsets+num) + chkLen(4B)`。读取端 `BytesToU32Slice` **零拷贝**把数组区转成 `[]uint32`。

解码端(`table.go::getBlock`)按尾部元数据逐段切开数据区并校验 checksum,才交给 blockIterator;块内二分(`blockIterator.Seek`)直接对 `entryOffsets` 二分,每次探测只 `loadIndex` 解码 key(不构造 Item、不拷贝)。

## 6.3 构建流程(SstBuilder)

`Add(e, isStale)`:

1. `tryNewBlock(e)`:`curBlock.estimateSize`(已用 + 新条目上界 + 尾部元数据)> `opt.BlockSize(4KB)` 且非空块 → `finishBlock()` 收尾旧块,开新块(data 预分配 4KB,不够按 `2×旧容量 / 已用+need+BlockSize 余量` 扩容——块尾追加只差几十字节时可免一次扩容);
2. 收集 bloom 哈希:`utils.Hash(ParseKey(keyTs))`(**去 ts**,同 userKey 多版本共享);
3. 维护 `maxVersion`;决定 baseKey(块首条 = 全量 key,后续条目与 baseKey 求 diff);
4. 编码 entryHeader + 追加;写 entryOffsets 记录;编码 value 进块。

`finishBlock()`:把 entryOffsets 数组、numEntries、checksum、chkLen 追加进块尾,块移入 `blockList`(之后写文件时数据区只用 `data[:endOffset]`)。

`done()`(flush 前调用):finish 收尾 → 由全部 keyHashes 生成布隆 → `buildBlockIndex` 序列化 TableIndex(同时累积各块数据区大小)→ 算索引 checksum → 汇总 `bd.size`。

### 布隆参数(utils/bloom.go)

- 位长公式:`bitsPerKey = ceil(-n·ln(fp)/ln²2 / n)`;`k = bitsPerKey×0.69`,钳制 [1,30];fp=0.01 → bits/key≈10、k≈7;
- 编码:`Filter = 位数组 + 尾 1B 存 k`(自描述);k 个 hash 位用**双重散列**派生(`delta = h>>17|h<<15`,逐位 `h += delta`),免去 k 次独立哈希;
- 探测方:`MayContainKey(k)` = `MayContain(Hash(k))`(探测 key 也要先哈希);k>30(未来新编码)保守返回 true 防误拒。

## 6.4 落盘(flush 与压缩共用)

`SstBuilder::flush`(经 `OpenTable(lm, name, builder)` 触发):

1. `OpenSStable` 以 `MaxSz = bd.size` 预分配文件并 mmap;
2. `Bytes(0, size)` 拿映射区,`bd.copy` **逐块直接拷进 mmap**(整表只此一次拷贝,不经过中间大缓冲——原实现 make(整表)双拷贝,是 flush 路径内存峰值的主因);
3. `SyncFile` 整区 msync。

`OpenTable` 读回时:`Init()` 解析表尾、校验索引;用**逆序迭代器**取全表最大 key 存 `sst.maxKey`(最小 key 直接取索引首个 BlockOffset.Key);`Table.maxVersion = TableIndex.MaxVersion`。

磁盘上 `SSTable`(裸文件)与逻辑 `Table`(持有 LevelsManger 引用、ref 计数、maxKey/minKey/maxVersion 缓存)分离;`Table.ref` 归零时:**先清空自己在块缓存中的所有块**(防悬垂),再 `sst.Delete()` 删文件。

## 6.5 Stale 数据标注

压缩输出时,被判为"墓碑/过期但必须保留"的条目走 `AddStaleKey`:除正常编码外累加 `staleDataSize`(key/value 与索引侧开销都要计入),随 TableIndex 落盘;读取端 `getStaleDataSize()` 供 Lmax→Lmax 压缩选择合并对象(第 11 步)。flush 产生的表 stale=0。

> 验证:`table_test.go` 的格式往返(构建→OpenTable→全量比对)、checksum 篡改报错、前缀压缩重建正确性(长公共前缀、空 dif、单 entry 块)。

---

# 第 7 步 点查路径与层间查找

> 目标:实现"给定 keyTs,在一张 SST 里找版本 ≤ ts 的最新条目",并把多表多层的查找串成 LSM.Get。单表路径同时也是迭代器 seek 的基础。

## 7.1 单表 Search 全流程

`Table::Search(keyTs)`:

```
1. bloom 剪枝: HasBloomFilter && !MayContainKey(userKey) → ErrKeyNotFound 直接返回
2. 索引二分:找第一个 baseKey > keyTs 的 block → 目标块 = 前一块(idx-1);
   idx==0(全表最小 key 都比目标大)→ 仍查第 0 块碰运气
3. getBlock(idx):块缓存取块(未中: mmap 零拷贝读 + 切尾 + 校验 checksum)
4. 块内二分(blockIterator.Seek):找第一个 ≥ keyTs 的条目
5. SameKeyNoTs 复核:是 → 命中(拷出 value);否 → ErrKeyNotFound
6. ErrBlockEOF 回退:目标 key 的版本可能排在"自己所在块"的前一块末尾
   (块边界恰好把同 userKey 的多版本切开)→ 回退到 idx+1 块再查一次
```

第 6 步的回退是**块边界语义**的必然:块按整 keyTs 切分,同 userKey 的高版本(小字节序)可能落在前一块,而目标版本在前一块的末尾之后。`searchBlock` 返回 `ErrBlockEOF` 表示"块内没有 ≥ keyTs 的条目",调用方据此决定回退。

## 7.2 块缓存键与缓存接入

- 缓存 key = `fid<<32 | blockIdx`(数值型,配合黄金比例分片,见第 8 步),缓存 value = 共享的 `*block`(block 内 entryOffsets 是 mmap 视图的零拷贝切片;value 区也是 mmap 视图)。
- 返回给用户的条目中 **Key 已拷贝为独立内存**(`blockIterator.setIndex`:arena 或 SafeCopy),**Value 仍指向共享缓存块**——调用方按需 `SafeCopy`(Table.Search 里拷,迭代器按 Item 契约交给上层)。这是缓存共享价值时必须明确的拷贝边界。
- `IsSetCache` 开关:Search 与用户迭代为 true;**压缩迭代为 false(不污染缓存)**。
- 表删除联动:`Table.DecrRef` 归零时逐块 `cache.Del(blockCacheKey)`,防止文件删除后缓存悬垂。

## 7.3 层间查找:LevelsManger.Get(lsm/levelsManger.go)

```
1. L0:表按 maxVersion 降序(新表在前)→ 逐表 Search,第一个"找到"即返回
2. L1..L6:每层表按 minKey 升序且互不重叠 → 层内二分定位唯一候选表(getTable:
   找第一个 MaxKey ≥ key 的表)→ Search
3. 版本可见性三层过滤(对任意层返回的 entry):
   a. version == startTs      → 精确命中,直接返回
   b. version ≤ startTs 且带 BitTxn → 该表已含"本 key 的最新可见版本",提前返回
   c. 其余(version > startTs,或 vlogGC 重写的非 BitTxn 旧版本)→ 继续向更下层找
```

为什么 (b) 安全而 (c) 必须继续:

- **BitTxn 条目**来自普通事务提交,commitTs 单调递增且只写进当时的最新表 → 存储层天然"新数据在上";若在较上层就找到 `≤ startTs` 的版本,更下层只可能更旧,**不可能存在对 startTs 可见的更新版本**;
- **vlogGC 重写条目**(`BatchSet`,无 BitTxn)会把"仍被引用的旧版本"复活进最新内存表(第 14 步),破坏"存储层新旧顺序 = 版本顺序"不变量 → 必须全层找最大;跳表内同 key 多版本共存时同理,`SkipList.Get` 返回的是"最接近 ≥ keyTs"的节点,其版本可能大于 startTs(不可见),需要继续下探。

每个候选层的搜索都遵循同一规则;全部层查完 → `ErrKeyNotFound`(此时"最接近"的版本都不可见或不存在)。

## 7.4 并发安全:先持引用,后放锁

- L0 查找:`RLock` 复制表切片并在**锁内逐个 IncrRef**,解锁后才 Search,defer 逐个 DecrRef——表可能正被压缩协程删除,引用保证文件在查找期间存活;
- Ln 查找:`getTable` 只能在 RLock 内调用,返回表后**仍在锁内 IncrRef** 再解锁(防止 RUnlock 后表被 DecrRef 归零删除);
- 内存表侧同理:LSM.Get 在 RLock 下取 memoryTable 与 immutable 引用并 IncrRef,倒序(新→旧)查找。

## 7.5 早期返回 vs 继续下探的一个完整例子

存储:`memtable(v9, 无 BitTxn 的 GC 重写)` + `L0(v11, BitTxn)` + `L1(v5, BitTxn)`,读取 startTs=10:

1. 内存表返回 v9(非 BitTxn)→ 不满足 (b),继续;
2. L0 返回 v11:version 11 > 10 → 不可见,继续;
3. L1 返回 v5:`5 ≤ 10` 且 BitTxn → 返回 v5。✅ 正确(可见的最新是 v5)。

若第 1 步就因"v9 ≤ 10"提前返回就会**读旧掩新**,这正是 vlogGC 重写条目不带 BitTxn 的原因。

> 验证:`db_test.go` 的多版本读、删除后旧值不可见、`read-path-optimization.md` 记录的相关优化过程。

# 第 8 步 块缓存 16 分片 W-TinyLFU

> 目标:SST 块(4KB)的读缓存。要求:命中率高、抗扫描污染、并发好。TrainKV 实现了一个浓缩版 **W-TinyLFU**:窗口 LRU + 分段 LRU + 门卫布隆 + Count-Min Sketch。

## 8.1 结构与容量分配

```
cacheShard ×16(分片;互不干扰,每片一把写锁)
├─ wlru  winLRU         窗口段,容量 15%
├─ slru  segmentedLRU   主段,容量 85%
│        ├─ stageOne:15%(准入观察区)
│        └─ stageTwo:85%(高命中保护区)
├─ door  BloomFilter    门卫:记录"是否至少见过 2 次"(fp=0.005)
└─ cmkt  cmSketch       频次估计:4 行 ×(2×每片条目数) 个 4-bit 饱和计数器
```

- 全局 `Cache` 由 16 个 `cacheShard` 组成;单锁在压缩与读并发下退化为 futex 竞争(profile 显示约占读路径 13%),分片后每片锁流量 1/16;
- 分片路由:`shardFor(hash) = (hash × 0x9E3779B97F4A7C15) >> 60`(黄金比例乘法取高 4 位)——块缓存 key 是 `fid<<32|blockIdx` 的结构化数值,直接取低位会集中到少数分片;
- 所有 list.Element 存 `storeItem{keyHash, value, stage}`,并有全局 `data map[uint64]*list.Element` 做 key→元素的索引(跨 wlru/slru 共享一张表)。

## 8.2 cmSketch 细节

- 4 行计数器,每行容量取 2 的幂;行内定位 `(keyHash ^ seed[i]) & mask`,每行用不同随机种子(异或后掩码);
- **一个字节打包两个 4-bit 计数器**(`index=keyHash/2`,`offset=(keyHash&1)*4`),内存≈1/4 计数器/条目;
- `increment` 饱和于 15;`Estimate` 取 4 行**最小值**(抑制碰撞高估);
- 衰减:`Reset()` 把所有 4-bit 计数器**减半**(`(x>>1)&0x77`),不是清零——周期性地让旧频次"半衰",兼顾近期性。

## 8.3 读路径(get)

1. 每片 `total++`,当 `total == threshold(=每片条目数×100)` → `cmkt.Reset() + door.reset() + total=0`(频率衰减时钟);
2. 无论命中与否都 `door.Allow(hash)` + `cmkt.increment(hash)`——**未命中也要计频**,冷门但反复访问的数据靠它积累"准入资格";
3. 命中后按 stage 分派:wlru 命中 → MoveToFront;slru stageOne 命中 → 晋升 stageTwo;stageTwo 命中 → 仅 MoveToFront(不降级);
4. stageOne→stageTwo 晋升若 stageTwo 满:取 stageTwo 队尾与当前元素**互换 storeItem 内容**,败者回 stageOne 头——由于换的是"内容",两个 list.Element 的 map 索引都要重写,**get 之后必须按 hash 重新解析 element**,否则会返回对方 key 的 value(代码注释的显式警告)。

## 8.4 写路径(set:准入控制)

1. key 已存在 → 仅更新 value;
2. 不存在 → 新 storeItem 进 wlru;
3. wlru 淘汰出候选 eitem → 若 slru 未满直接加入;若满:
   - 取 victim = slru.stageOne 队尾(最冷观察者);
   - **PK 准入**:`door.Allow(hash)` 必须为 true(该 key 至少被见过 2 次,防一次性扫描数据入缓存),且 `cmkt.Estimate(候选) ≥ Estimate(victim)` → 候选晋级;否则丢弃候选;
   - slru.add 内部若总容量满,复用 stageOne.Back() 元素(原地改写,零分配)。
4. 删除(del):按 stage 从对应 list 移除并删 map(表删除时逐块调用)。

与标准 TinyLFU 的差异:无采样器(改为"未命中也计频")、victim 淘汰后不真删(由 slru.add 就地覆写)、全程写锁但靠分片降竞争。

## 8.5 与 LSM 的集成(lsm/cache.go)

- `LevelsCache{blockData *cache.Cache}`,默认 `CacheNums = 10240` 条目;
- 块 key = `fid<<32|blockIdx`(数值路由);value = 共享 `*block`;
- 压缩与恢复路径 `IsSetCache=false` 不喂缓存;点查与用户迭代喂缓存;
- `Table.DecrRef` 归零时 Del 自己的全部块,防悬垂。

> 验证:`utils/cache/cache_test.go`(准入/晋升/衰减/删除语义)、`benchmk/` 的读放大测试。

---

# 第 9 步 层级管理与 MANIFEST

> 目标:管理 L0–L6 七层的表集合与元数据,并解决"进程重启后,磁盘上哪张表属于哪一层"的登记问题。

## 9.1 Level 语义与不变量

| 层 | 内容排序 | 表间关系 | 层内查找 |
|---|---|---|---|
| L0 | **maxVersion 降序**(新表在前) | 允许重叠(flush 直接落 L0) | 逐表 Search,先中先得 |
| L1..L6 | **minKey 升序** | 层内互不重叠(有序) | 二分定位唯一候选表 |

`LevelHandler`:`tables []*Table + totalSize + totalStaleSize + RWMutex`。所有增删/排序在锁内完成;`addSizeLocked/subtractSizeLocked` 维护两个总量(压缩分数与 stale 分数用)。

层排序在以下时机各执行一次:启动 build、flush 后(`addAndSize`)、压缩提交后(`updateTable/deleteTable`)。

## 9.2 MANIFEST:表登记文件(lsm/manifest.go)

**文件格式**(追加式):

```
| MagicText 'MAGC' 4B | version 4B(=1) |            ← 8B 文件头
| len 4B BE | crc32 4B BE | protobuf ManifestChangeSet |   ← 每条变更
| len 4B BE | crc32 4B BE | protobuf ManifestChangeSet |   ← …
```

变更原语只有两种:`Create{Id, LevelId, Checksum}` 与 `Delete{Id}`。`ManifestChangeSet` 一批可含多条(压缩一次提交:新表 Create×N + 旧表 Delete×M)。内存侧 `Manifest{Levels[][TableIds], Tables map[id]TableManifest, Creations, Deletions}` 实时应用。

**写路径**(`addChanges`):

1. 先应用到内存 manifest(持 mux);
2. 若删除过多(`Deletions > 10000 且 Deletions > 10×(Creations-Deletions)`)→ **重写**:关旧文件 → 全量状态写进 `REWRITEMANIFEST`(一条大 changeSet)→ Sync → rename 覆盖 `MANIFEST` → SyncDir;
3. 否则追加 `[len|crc|data]` 一条并 `f.Sync()`(每次变更都 fsync——压缩/flush 提交的持久化边界)。

**读路径**(`ReplyManifestFile`):校验头 → 逐条读 `len|crc` → 校验 crc → `applyChangeSet`(Create 重复=错误;Delete 不存在=错误)→ 返回"最后一条完整记录的偏移",**启动时 `Truncate` 掉尾部半条**(宕机窗口)。

**启动对账**(`checkSSTable`):manifest 有而目录没有 → 报错(不允许静默丢表);目录有而 manifest 没有 → **删孤儿 .sst**(压缩提交中间态的残留,第 15 步)。

## 9.3 启动 build(lsm/levelsManger.go::build)

1. 建 7 个 LevelHandler;
2. `checkSSTable` 对账;
3. 初始化块缓存;
4. 遍历 manifest.Tables:逐个 `OpenTable`(打开文件 + Init 索引 + 逆序迭代求 maxKey)→ `addAndSize` 挂到对应层;
5. 每层 Sort;`maxFID` 取最大者(与 wal 的 fid 同空间,供 recovery 判重)。

## 9.4 表集合变更的引用语义

- **flush 提交**:`AddTableMeta(0, {fid, checksum:'skip'})` → `levelHandlers[0].addAndSize(t)`(OpenTable 时 ref 已 =1,层持有该引用);
- **压缩提交**(runCompactDef):`manifestFile.AddChanges` → `nextLevel.updateTable(删nextTables, 加buildTables)`(buildTables 在 OpenTable ref=1 基础上再 IncrRef=2,随后 compactBuildTables 的 decrRefs 释放临时引用,剩 1 归层持有)→ `thisLevel.deleteTable(thisTables)`(摘除并 decrRefs,读者引用归零后文件才真正删除);
- 文件删除永远发生在 **ref==0** 时(读者/迭代器可能仍在用 mmap 视图,引用计数挡住删除)。

## 9.5 为什么 flush 先登记 manifest 再挂层

`levelsManger.flush` 顺序:建 5.sst(msync 落盘)→ manifest 登记(落盘)→ addAndSize。若在"登记后、删 wal 前"宕机:重启时 5.sst 已从 manifest 恢复,残留 5.wal 被 recovery 按 `fid == sstMaxID` 规则删除(第 5.4 节);若在"建表后、登记前"宕机:manifest 没有 5 → checkSSTable 删孤儿 5.sst,而 5.wal 还在 → 重放后重新 flush 出 5.sst(同一 fid,幂等)。两个窗口都有明确出口,这正是"wal 与 sst 共享 fid"设计的好处。

> 验证:`crash_test.go` 在 flush/manifest 各窗口杀进程后重启对账;`manifest.go` 的 rewrite 阈值单测。

# 第 10 步 迭代器体系与合并读

> 目标:提供统一的有序遍历能力(升/降序、Seek、前缀),让"多表、多层、含内存表"的合并读与压缩共用一套代码。

## 10.1 四级迭代器职责

| 迭代器 | 遍历单位 | 职责 |
|---|---|---|
| `blockIterator` | 块内 entry | 解码前缀压缩 key、entryOffsets 二分 Seek、Item 构造(拷贝 Key;Value 为 mmap 视图) |
| `TableIterator` | 单表(跨块) | 块间跳转;Seek 语义"必返回第一个 ≥ key 的值";进块时按 IsSetCache 决定是否喂缓存 |
| `ConcatIterator` | 层内多表 | 按表顺序串联:升序正接、降序反接;Seek 先二分定位表 |
| `MergingIterator` | 多路来源 | 堆式 k 路归并;Less 规则即全局排序规则 |

`NewLsmIterator`(LSM 全量迭代器)的来源顺序:

1. 可写 memtable;
2. immutable 表(新→旧);
3. L0:每张表**逆序**(旧→新)展开为独立 TableIterator(层内重叠,必须逐表归并);
4. L1..L6:每层一个 ConcatIterator(层内无重叠,串联即可)。

MergingIterator 的堆(Less 规则):比较 `CompareKeyWithTs(Item.Key)`——**升序**下,同一 userKey 里大 ts(新版本,字节序小)先出,天然"新版本在前"。

## 10.2 块/表级 Seek 语义的微妙之处

- `TableIterator::seekFrom` 与 Search 同款二分:定位"第一个 baseKey > key"的块,向前一块 Seek;若前一块返回 `ErrBlockEOF`(块内没有 ≥ key 的条目),再跳到后一块取首条"顶替"——保证 **Seek 的返回 ≥ key**,哪怕 key 落在两块的间隙;
- `SeekPrev`(降序)= Seek 后检查 userKey 是否相同,不同则 Prev 一格;
- 每次跨块时 `biter.data = nil` 标记"需重新加载块",递归推进;`TableIterator.Valid()` 以 `err == nil` 判断(ErrBlockEOF 即遍历结束)。

## 10.3 Item 生命周期契约(容易写错的地方)

- 块内条目解码时 **Key 拷入独立缓冲**(有 `ChunkedArena` 用 arena 切片,无则 SafeCopy),保证两次 Next() 之间 Item.Key 稳定;Value 指向共享 mmap/缓存块(表存续期间稳定);
- 跳表迭代器 Key/Value 是 arena 视图(只追加不迁移,稳定);
- MergingIterator 直接透传堆顶 Item(零拷贝),因此**前提是子迭代器 Item 在两次 Next() 之间有效**——上面两条保证满足;
- `Rewind` 会放弃 arena(契约:Rewind 前所有 Item 已消费),回收整块内存。

## 10.4 降序遍历的构成

降序 = TableIterator 逆序(SeekToLast/Prev)+ ConcatIterator 反接 + 跳表迭代器 Prev 语义。`interfaces.Options{IsAsc, IsSetCache, Prefix}` 贯穿所有迭代器;压缩固定用 `{IsAsc:true, IsSetCache:false}`。

> 验证:`mergeSort_test.go`、`merging_iterator_test.go`(乱序交错 key 的归并正确性、多版本次序)。

---

# 第 11 步 Compaction 分层合并与版本回收

> 目标:L0 无限堆积会拖垮读(层内表数线性放大),旧版本不清理空间无限增长。压缩把重叠数据向下归并、清理墓碑与旧版本,同时统计 vlog 死指针驱动 value 日志 GC。这是全项目最复杂的一步,分"调度、选表、执行、版本回收"四个子问题。

## 11.1 触发机制

- 后台:`NumCompactors(默认 4)` 个协程,启动先随机延时 0~1s,之后每 **50s** 周期执行一轮 `RunOnce`;compactor 0 会把 L0 优先级提到最前(`moveL0toFront`),保证 L0 及时下沉;
- `RunOnce` 已导出,测试/运维可手动触发(很多测试直接调它);
- 每轮执行顺序:算各层分数 → 排序 → 逐个尝试执行,`prio.adjusted < 1.0` 的(除 L0 特例)停止本轮。

## 11.2 分数与目标层计算

**levelTargets**(每轮先算一次):

1. 从最底层向上反推:设最底层当前总量为 totalSize,上层理论目标 = 下层/`LevelSizeMultiplier(10)`(自底向上形成 10 倍递减的金字塔);
2. 若某层理论目标 ≤ `BaseLevelSize(10MB)`,它就是 **base level(目标层 Y)**:L0..Y 直接归并过去;
3. `fileSize[i]`:L0 = MemTableSize,`i ≤ Y` 层 = BaseTableSize(2MB),更深层逐层 ×`TableSizeMultiplier(2)`(越深文件越大)——压缩时按此切新表;
4. 若 Y 层为空且 Y+1 层也吃得下(Y+1 总量 < fileSize[Y+1]),直接跳层,让数据一次沉到位。

**每层分数**(pickCompactLevels):

- L0:`score = 表数 / NumLevelZeroTables(5)`;
- Ln:`score = (层总量 − 压缩中剔除量 delSize) / levelTargetSSize[n]`;
- **调整**(adjusted):自 base level 向上,若下层 score>1,则上层分数除以下层分数——下层越紧张,上层越不被选中(把压缩预算让给下层),反向则反之;最后按 adjusted 降序执行。

## 11.3 四类压缩任务与选表

| 类型 | 条件 | 做法 |
|---|---|---|
| L0→Y | prio.levelId==0 且 Y>0 | 从新到旧取**连续重叠**的表组(keyRange 并集),找到 Y 层与之重叠的全部表 |
| L0→L0 | Y==0(仅 compactor 0) | 收集 ≥4 张满足:size<2×fileSize[0]、创建 >10s、未在压缩中的表 → 合成大表(`thisRange = {inf}` 强制重叠,fileSize 放大到 4GB 防止提前切表) |
| Lx→Lx+1 | 非末层 | 表按 maxVersion **升序**(老数据优先清理)逐个尝试:选一张 t + 下一层重叠表区间;若 t 与下层无重叠 → 直接下沉(迁层) |
| Lmax→Lmax | 末层 | 表按 staleDataSize 降序;要求 `MaxVersion ≤ discardTs`(无活跃读者)、创建 >1h、stale ≥ 10MB(`LevelMaxStaleDataSize`);单表够大直接选,否则顺带相邻表凑 `fileSize`;stale 为 0 的末层表永不压缩 |

每类最终都通过 **compactIngStatus.compareAndAdd(CAS)** 登记"本层/下层被占用的 key 区间 + 参与表",防止多压缩协程重叠;同一张表同时在多任务中会被 `tables` 集合挡住。登记的区间在任务结束时 `deleteCompactionDef` 释放。keyRange 的 `left/right` 由参与表范围外扩成**含全部版本的超区间**(`KeyWithTs(minUserKey, MaxUint64)` ~ `KeyWithTs(maxUserKey, 0)`),保证区间判定不漏任何版本。

## 11.4 并行执行:split 与 Throttle

`runCompactDef`:

1. `addSplits`:按下层表数把合并区间切分成若干子区间(`width = max(3, ceil(len(nextTables)/5))`,右边界取某表 maxKey 的 **userKey+ts=0**,即"该 userKey 的所有版本都归入左侧",子区间间无版本割裂);
2. `discards := getDiscardTs()` **提前快照**(任务可能跑几秒,水位可能推进,快照保证同一 key 的前后版本按同一水位判定);
3. 每个 split 起一个 subCompaction goroutine;总并发受 `Throttle(8+len(splits))` 限制(既是并发闸,又让任一子任务失败后其余 Do() 提前失败退出);
4. 子任务产出表经 `res chan` 汇合,全部 Finish 后 `SyncDir` 落目录,新表按 maxKey 排序返回。

## 11.5 subCompact 归并与版本回收(核心逻辑)

每个子任务:`newIterator()` = 源层表迭代器(L0 源逐表逆序;Ln 源单表)+ 目标层 ConcatIterator → MergingIterator → `addKeys(builder)` 逐个消费:

1. **skipKey 跳过**:一旦决定"本 userKey 之后的版本全部丢弃",后续同 key 条目只计数(`numSkips`)并 `updateDiscardStats`(是 vlog 指针就累计该 fid 的 discard 字节),不再写入;
2. **跨 key 边界**:记录 `lastKey`;检查右区间(超过 split 右界停);`builder.ReachedCapacity()` 停(本 key 组已完整进当前 builder,新 key 留给下一个 builder——**保证同 key 多版本决策不跨表拆分**);每 `SSTKeyRangCheckNums(5000)` 个 key 检查一次"与下下层表的重叠数 ≥10"(`exceedsAllowedOverlap`)→ 提前切表,避免在 Li 生成与 Li+1 大量重叠的表(防写放大);
3. **版本回收判定**(本 key 组内,升序遍历 = 新版本先到):
   - 只统计 `version ≤ discards` 的低版本数 `keyNumVersions`(**水位以上的版本可能正被活跃事务读,绝不能动**);
   - 当低版本数到达 `NumVersionsToKeep(1)`(即到达"最老的可保留版本"),或者当前条目是墓碑/过期:
     - 墓碑 + 与更下层有重叠(`hasOverlap`,该 userKey 在更深层还有数据)→ 墓碑**必须保留**(删了会暴露下层旧值);
     - 墓碑 + 目标层是末层 → 保留为 **stale**(末层没有更下层,删除语义靠它表达;留着由 Lmax→Lmax 统一回收);
     - 其余情况(墓碑无深层重叠 / 过期 / 保留版本数已够)→ **丢弃**,同 key 更旧版本全部跳过;
   - 保留的墓碑/过期条目用 `AddStaleKey`(计入 staleDataSize);
4. **hasOverlap 判定**:`checkOverlap(cd.allTables(), nextLevel+1)`——检查本任务覆盖的 key 范围在**比目标层更深**的层里是否还有表(有 → 本范围内不是全部版本,不能随便删墓碑)。

为什么这套规则安全(和 12 步的水位闭环):

- 版本 > discards:可能有活跃读事务的 startTs ≥ 它 → 全保留;
- 版本 ≤ discards:所有 startTs ≤ discards 的事务都已结束;只保留每个 key 最新的 NumVersionsToKeep 个,更旧的物理删除——任何仍可能发生的读(其 startTs > discards)只会读到保留的最新版本,不会读到被删的旧版本。

subCompact 结束前把本 split 的 `discardStats map[uint32]int64` 通过独立 goroutine(`updateDiscardStats`,纳入 discardStatsWG,关闭时等待)投递到 `DiscardStatsCh`(容量 16,阻塞式投递),vlog GC 消费(第 14 步)。

## 11.6 新表落盘与元数据提交顺序

每个 builder 满后异步(Throttle 内)`:NextFileID → OpenTable(写文件+msync) → res <- t`。全部完成后的提交顺序:

```
manifestFile.AddChanges(create×新表 + delete×旧表)  ① 持久化登记
nextLevel.updateTable(删 nextTables, 加 buildTables) ② 挂新表(层内 IncrRef)
thisLevel.deleteTable(thisTables)                    ③ 摘旧表(decrRefs)
```

①必须在②③之前且先落盘:一旦①持久化,压缩结果即"不可撤销"。若在①之后、③之前宕机:manifest 已登记新表并标记删除旧表,而旧表文件还在磁盘——重启时 build 按 manifest 只加载新表,残留旧表被 checkSSTable 当作孤儿删除(压缩输出已含全部有效数据,不丢);若宕机发生在①之前(新表已落盘但 manifest 未写):新表成为孤儿被删,旧表原样恢复,任务下次重来。反之若先改内存层再写 manifest,宕机会出现"manifest 里有表但内存层没挂上/旧表还挂在层里却已被 manifest 删除"的不一致。

**stale 表的删除时机**:stale 数据在 Lmax 停留,必须满足"表 maxVersion ≤ discardTs 且创建超 1h 且 stale ≥10MB"才被选中重写——压缩重写时旧版本/墓碑在 `version ≤ discards` 条件下被彻底丢弃(墓碑如已无深层意义),stale 归零的末层表不再被选。长时间没有新写的冷数据通过这个机制最终被清掉。

## 11.7 其他细节

- `findTables(Ln→Ln+1)` 中源层单表与目标层无重叠时"直接下沉"仍走完整合并(输入输出内容一致,但表下沉一层);
- 压缩输出表 `builder.maxVersion = sstMaxVersion` 显式回填(索引里的 MaxVersion 参与 L0 排序与水位判断);
- 打印日志:耗时 ≤1s 的压缩打印输入输出表清单(性能测试时可关)。

> 验证:`compaction_paths_test.go`(四类任务的路径)、`compaction_gc_test.go`(压缩+GC 联动、版本回收后读不到旧值)、`db_test.go` 的压缩压力用例;`-race` 下多 compactor 并发压缩。

# 第 12 步 事务与 MVCC

> 目标:在 LSM 之上提供快照隔离读写事务。两个时间戳(读 startTs / 写 commitTs)、两个水位(谁在读、谁提交完)、一组哈希表(读集/写集/冲突集)。事务原子性由"WAL 组标记 + 单写协程"保证(第 4/5/13 步),不需要 undo log。

## 12.1 时间戳模型

- `TransactionManager.nextTxnTs` 单调递增(tsLock 保护),启动时播种为数据最大版本 `MaxVersion()`+1;
- **startTs**:事务创建时取 `nextTxnTs-1`,读快照 = "只能看到 version ≤ startTs 的提交";**commitTs**:提交时取 `nextTxnTs` 并自增;
- 版本号是全局单调的提交序,也是数据的新旧序:commitTs 越大,keyTs 编码越小,排得越靠前;
- 普通事务提交的条目全部带 `BitTxn`。

## 12.2 LimitMark:单消费协程水位机(utils/limiterMark.go)

```go
type LimitMark struct {
    markCh    chan mark      // 容量 100 的消息管道
    lastIndex atomic.Uint64  // 最新见过的 index
    doneIndex atomic.Uint64  // 已确认全部结束的水位(只前进)
}
```

Begin(x)/Done(x) 只是向 markCh 投递 `{index, done}`;**单消费协程 processOn** 串行处理:

1. `indexDoneNum[index] += ±1`(首次见则入最小堆);
2. 从堆顶开始:只要最小 index 的计数 ≤ 0,就弹堆并推进 `curIndex`;
3. doneIndex 单调 CAS 推进(若被推进,向 `doneIndexCh` 异步通知外部——levelsManger 收到后更新 `txnDoneIndex`,即压缩的 `getDiscardTs()`);
4. `WaitForIndexDone(ctx, index)`:doneIndex ≥ index 立即返回;否则向 markCh 投 `{waiter}` 挂起,水位越过 index 时(区间遍历或 map 遍历,按密集/稀疏分支)close 等待通道唤醒。

语义:**doneIndex = X ⟹ 所有 ≤ X 的 Begin 都已收到对应的 Done**。由此一个原语服务两类消费者:startMark 回答"≤X 的读事务都结束了"(压缩据此安全回收版本);commitMark 回答"≤X 的提交都写完了"(新读事务据此保证可见性)。手写最小堆避免 container/heap 的 interface 装箱。

## 12.3 事务创建与水位等待策略

`NewTransaction(update)`(事务对象走 sync.Pool,取出必须复位残留字段!):

- `startTs = manager.startTs(update)`,其中 `startMark.Begin(startTs)`(登记一个活跃读);
- **纯读事务**立刻 `commitMark.WaitForIndexDone(startTs)`——"我承诺看到 ≤ startTs 的所有提交",所以必须等它们全部写完;
- **写事务不付这笔等待**(startTsReady=false),把承诺**延迟到第一次真正读 LSM 时**(`ensureStartTsReady`):纯写事务整个生命周期可能从不读 LSM,省一次水位等待;
- 关闭/清理:`Discard` 时 `doneStart`(startMark.Done)→ 水位推进,压缩才知道可以清理。

启动播种:`Open` 里对两个 Mark 各做一次 `Done(nextTxnTs)`(无 Begin 的 -1 计数)→ doneIndex 直接顶到 MaxVersion:旧数据立即可回收;此后首个事务 startTs = MaxVersion,其可见版本恰是各 key 最新版,而压缩始终保留每 key 最新的 NumVersionsToKeep 个低水位版本,边界安全(第 11.5 节的规则)。

## 12.4 提交与冲突检测

`commitAndSendToDB`(全程持 `manager.writeChLock`,保证提交临界区串行):

1. `newCommitTs(txn)`:tsLock 内先 `hasConflict` 检查:
   - 遍历 `commitedTxns` 中 `commitTs > 本事务 startTs` 的历史提交(即本事务开始之后完成的提交),若本事务**读集 readKeys** 与某提交的**写集 conflictKeys** 有交集 → `ErrConflict` 返回(读-写冲突:我读的数据在我开始后被别人改了);
   - 键用**双哈希**(MemHash + xxhash)登记,消除单 64 位哈希碰撞的误报;
2. 无冲突:`doneStart`;`cleanCommitedTransaction()` 按 startMark 水位清理过期提交记录;取 commitTs、`commitMark.Begin(commitTs)`;
3. 组装提交批次:`pendingKeys` 里的每条 Entry 打上 commitTs、key 换成 `KeyWithTsPooled`、**置 BitTxn**,按 key 排序(利于 WAL 重放时跳表插入局部性;单条目跳过排序);追加 finTxn 结束标记条目(复用对象 + 复用 ts 缓冲);
4. `SendToWriteCh` 投递(返回的闭包在请求落盘完成后 `commitMark.Done(commitTs)`,同时释放写通道锁让下一事务进入——**先放行下一事务的临界区,再等自己的落盘**,临界区只保护"分配 ts/登记冲突/组批次",不含磁盘 IO);
5. `Commit()` = 投递 + 闭包等待(返回 commitTs,供调用方复用)。

写集/读集都只是哈希:`conflictKeys map[hash]struct{}`、`readKeys []uint64`(读取 LSM 前记录,读己之写命中则不算读 LSM)。提交后冲突快照拷入 `commitedTxns`,事务对象即可池化复用。

## 12.5 事务内读与读己之写

`txn.Get`:

1. 先在 `pendingKeys`(键 = MemHash(userKey),用 bytes.Equal 复核防碰撞)找本事务写过的 key——**读己之写**;若事务内写过删除则返回 ErrKeyNotFound;
2. 写事务记录 readKey(冲突检测用),`ensureStartTsReady()`,然后 `db.get(KeyWithTs(key, startTs))` 走 LSM 全链路(vlog 大值展开);
3. 删除墓碑/已过期 → ErrKeyNotFound。

## 12.6 事务迭代器(TxnIterator)

`txn.NewIterator`:合并 LSM 全量迭代器 + **pendingWrites 迭代器**(本事务未提交写,以 startTs 为版本参与归并),然后:

- 可见性过滤(`IsVisible:startTs ≥ version`)与墓碑跳过;
- 大 value 惰性化:只留 ValuePtr,`Item.Value()` 按需从 vlog 读(扫大 value 不逐条拷贝);
- pending 覆盖:LSM 命中同 key 时,若本事务 pendingKeys 里有,则换成 startTs 版本(读己之写的一致性);
- 同 userKey 多版本去重(升序新版本先到,只返回第一个);
- 迭代计数保护 vlog GC 的文件删除(见第 14 步);`txn.Discard` 时若有未关闭迭代器直接 panic。

## 12.7 与压缩的版本回收闭环(串联)

```
读事务创建:startMark.Begin(startTs)
读事务结束:startMark.Done(startTs)  → processOn → doneIndex → TxnDoneIndexCh(容量1)
                                        → levelsManger.txnDoneIndex = discardTs
压缩执行:先快照 discards;组内 version ≤ discards 的旧版本按 NumVersionsToKeep 丢弃;
         选末层表要求表 MaxVersion ≤ discardTs(该表不可能再被任何活跃读事务读到)
```

一条链保证:**任何被物理删除的版本,都不再可能被任何已开始或未开始的读事务读到**。

> 验证:`txn_test.go`(读己之写、冲突 abort、提交后可见、回滚释放)、`db_test.go` 的读写并发、`-race`。

# 第 13 步 写通道与 Group Commit

> 目标:把三类写者(事务提交、vlog GC 重写、discard 统计持久化)收进同一根管道,串行化"编码 → vlog → LSM"落盘,并发提交攒批摊销 fsync。所有提交方通过 Wg/Err 等待结果。

## 13.1 Open 启动顺序(db.go::Open)

1. `CheckOpt`:WorkDir 为空自动建临时目录(Close 回调删除);
2. `flock.TryLock(LOCKFILE)`:失败返回 ErrLockDB(单进程互斥);
3. `initVlog` + `opt.DiscardStatsCh = &vlog.FlushCh`(压缩 → vlog 统计通道);
4. `opt.TxnDoneIndexCh = make(chan uint64, 1)`(startMark → 压缩水位);
5. `NewLSM`(第 5 步:init levels/manifest → 起 flush 协程 → recovery 重放 WAL → 重放的表重新入 flush 队列);
6. `transactionManager`:nextTxnTs 播种 MaxVersion、两 Mark 播种、increment(第 12 步);
7. 起 `handleWriteCh` 单消费者 + `vlog.waitOnGC` 门栓协程;
8. `vlog.Open`:扫描/打开 .vlog(最后一个文件迭代到有效末尾定位可写 offset;读回上次持久化的 discard 统计表);
9. 起 `NumCompactors` 个压缩协程。

**注意 LSM 的 recovery 发生在 vlog.Open 之前**——vlog.Open 需要 LSM 已可读(sendDiscardStats 要查特殊 key)。

## 13.2 handleWriteCh:攒批 + 写令牌(db.go)

单消费者循环,维护一批 `reqs`:

1. 收到请求先内层 for 继续收(`collected` 循环)直到通道空或攒满 `WriteChBatchThreshold(900)`(写通道容量 1000,阈值必须小于容量);
2. `SyncWrites` 且**存在并发提交迹象**(本批 >1 或上一批 >1)时,开 **1ms 汇合窗口**(`SyncGroupCommitWait`)再收一会儿——串行提交不加延迟,并发提交让一次 fsync 覆盖更多请求(真正的 group commit 窗口);
3. 写盘本身也串行化:容量 1 的 `blockChan` 令牌。令牌空闲 → 本协程直接 `WriteRequest`(省一次协程切换,串行提交的主路径);令牌被占 → `go writeRequest(reqs)` 排队,本协程**立刻回到外层继续攒批**(写盘不阻塞收集);
4. 错误只记录不退出(退出会让 writeCh 失去消费者,后续提交死锁);
5. Close 信号:先排空通道内所有请求再写最后一批。

## 13.3 WriteRequest:一次串行落盘(db.go)

```
vlog.Write(reqs)                 ① 大 value 编码进 vlog(mmap)
SyncWrites && wrote → vlog.Sync() ② vlog 先落盘:WAL 里的指针必须指向已持久化的 value
逐个 req → writeToLSM(req)        ③ 每个 entry:小 value 直写 / 大 value 替换成 12B 指针
                                   → lsm.Put → WAL(mmap)+ 跳表
SyncWrites → SyncWalFile()        ④ WAL 落盘
doneRange(nil/err, from, to)      ⑤ 逐个 Wg.Done + 填 Err
```

错误处理语义(`doneRange`):**按 [from,to) 区间标记**——第 i 个请求失败时,它之前的请求已完整写入(内存)标记成功,失败点及之后标记失败。为什么重要:若把已成功的请求也标失败,调用方(事务层)可能重试产生**重复版本**。SyncWrites 下失败路径会补一次 WAL 同步,兑现失败点之前请求的持久化承诺(失败请求可能留下的半组 BitTxn 记录在恢复时被整体丢弃,不影响一致性)。

## 13.4 writeToLSM 与价值分离点

`ShouldWriteValueToLSM(e)` = `len(value) < ValueThreshold(1MB)`:

- 小 value:清掉可能的 BitValuePointer,value 原样进 LSM;
- 大 value:置 BitValuePointer,value 替换为 `req.EncodeValPtr(ValPtr[i])` 的 12B(写请求级复用缓冲),进 LSM 的只是一条小记录;
- 进跳表前 `lsm.Put` 做轮转预判(第 5 步)。

## 13.5 Close 顺序(生产者先停,消费者排空)

```
① valueGC 停(GC 重写是 writeCh 生产者)
② compactors 停(压缩的 discard 统计是 FlushCh 生产者)
③ transactionManager.Stop(事务提交是 writeCh 生产者;等两个 LimitMark 协程退出)
④ vlog.stopDiscardStats(统计刷盘协程排空退出)
⑤ writes(handleWriteCh)停:排空 writeCh 中全部请求并落盘
⑥ CloseFlushIMemChan + memtable closer:flush 协程把 immutable 全部刷完
⑦ LSM.Close(关表、层、manifest)→ vlog.Close(截断/删空文件)→ 解锁
```

顺序错乱的后果:任何"生产者比消费者先停"之外的次序,都会让 writeCh 里的请求无人消费(提交方永久阻塞)或 flush 队列残留。

> 验证:并发提交压力测试下无重复版本(`db_test.go` 多协程 Update 同 key 计数)、`crash_test.go` 在提交窗口的持久化语义、SyncWrites 开关下的 fsync 行为。

---

# 第 14 步 Value Log 大 Value 分离与 GC

> 目标:超过 `ValueThreshold(1MB)` 的 value 不进 LSM,而是追加到独立的 value log;LSM 只留 12B 指针。好处:内存表/SST 变小、压缩只搬指针(写放大与 value 大小解耦)、大 value 写入只需一次顺序追加。

## 14.1 文件格式与轮转(vlogFile.go)

单条记录布局**与 WAL 相同**:`EntryHeader | key | value | crc32`(vlog 中 key 带 ts;Meta 已清掉 BitTxn/BitFinTxn——vlog 不需要事务组语义)。每条记录整体长度 = headerLen + klen + vlen + 4,存进 ValuePtr.Len。

- fid 自 1 独立分配(`createVlogFile`),与 LSM 的 fid 无关;文件名 `%05d.vlog`;新建文件 `MaxSz = 2×ValueLogFileSize` 预分配 + mmap + `MadviseSequential`;
- 轮转条件(二者任一):当前文件 `writeOffset ≥ ValueLogFileSize(默认 1GB-1)` 或 `entriesWrittenNum > ValueLogMaxEntries(1000)`——**按条数轮转**是为了让 GC 的粒度可控(每条目都要对照 LSM 查询,文件过大单次 GC 太久);
- `DoneWriting(offset)`:SyncWrites 时先 SyncRange 落盘,然后**持文件写锁** Truncate 收尾并重新映射(锁防 unmap/remap 期间并发读);
- 写入 `EncodeEntryAt`:直接编码进 mmap + 就地 crc(大 value 免中转拷贝),随后 `SetSize` 推进文件可见大小;`Write` 前有 `validateWrites` 预演(按真实轮转规则模拟,单请求不可跨文件,超 `ValueLogFileMaxSize` 直接拒)防"写一半发现装不下";
- 同步:`VLogFile.Sync` 用 `SyncDirtyRange(lastSynced, size)` 增量 msync。

## 14.2 读路径:零拷贝 + 读锁回调

`ValueLog.Read(vp)`:

1. `filesLock.RLock` 定位文件(**防文件被 GC 删除**),`maxFid` 文件还要校验 offset < 可写水位;
2. 持 `VLogFile.Lock.RLock`(防 remap/删除),`Bytes(offset, len)` 拿映射区零拷贝切片;
3. 返回 `(buf, callBack=RUnlock)`:调用方必须读完后调用 callBack(释放文件锁),**且不能把 buf 带出锁外使用**(需 SafeCopy);
4. `VerifyValueChecksum` 开启时校验 crc(默认关,读路径零额外开销)。

这个"锁 + 回调"契约是 GC 能安全删除文件的前提(14.4)。

## 14.3 discard 统计:压缩 → vlog GC 的桥梁

- 压缩丢弃任何带 ValuePtr 的条目时,把 `vp.Fid` 的 `vp.Len` 累加进本任务 discardStats(第 11.5 节)→ 投递 `FlushCh`;
- `handleDiscardStats` 协程消费:合并进内存 `FileMap[fid]`,**累计超过 100 次更新**(`discardStatsFlushThreshold`)就把整个 FileMap JSON 序列化,以普通 Entry 写回 LSM——特殊 key = 常量 `VlogFileDiscardStatsKey("VlogFileDiscard")`,version 取 `MaxUint64`(编码后字节序最小,永远排在该 userKey 最前,**普通事务快照读不到它**);**discard 统计因此持久化,重启不丢**;
- 启动时 `sendDiscardStats` 从 LSM 读回该 key(可能是 vlog 指针,需展开)反填 FileMap。

## 14.4 GC 全流程(vlog.go)

**入口** `RunValueLogGC(discardRatio)` → `runGC`:`GarbageCh` 容量 1 门栓(同时只有一个 GC;关闭时 waitOnGC 协程塞入哨兵禁止再启动)→ `pickVlogFile`:

- 排除正在写的 maxFid 文件;遍历 `FileMap` 找 discard 最大的候选;
- 候选 discard ≥ `discardRatio × 文件实际大小`(如 ratio=0.5 表示该文件一半以上是死的)才动手,否则 ErrNoRewrite。

**执行** `doRunGC → gcReWriteLog(logFile)`:逐条迭代目标文件(复用解码缓冲),对每条 vlog 记录做**去留判定**:

```
lsmEntry = LSM.Get(vlogEntry.Key)          // 取 ≤ 该版本的最新条目
├─ ErrKeyNotFound            → 丢弃(该版本已被压缩清掉)
├─ IsDiscardEntry(对照判定)   → 丢弃,条件:
│    ① vlog 版本 ≠ lsm 版本      (LSM 里已是更新的版本)
│    ② LSM 已是删除/过期墓碑
│    ③ LSM 该 key 已不再是指针    (后来被小 value 覆盖,值内联进 LSM 了)
├─ 解码 lsmEntry 的 vp:
│     vp.Fid==本文件 && vp.Offset==本记录偏移 → 存活:攒批重写(见下)
│     vp.Fid>本文件 或 vp.Offset>本记录偏移   → LSM 指向更晚文件/更晚偏移的更新副本,本条已死,丢弃
│     vp.Fid<本文件 或 vp.Offset<本记录偏移   → LSM 指向更旧记录(本记录是死版本),丢弃
```

存活条目攒批(按 MaxBatchCount/MaxBatchSize 分批,超大批 ErrTxnTooBig 时自动减半)经 `BatchSet → writeCh` **重写到当前 vlog 末尾**,并更新 LSM(新指针)与 WAL——注意重写条目**不带 BitTxn**,这正是第 7 步"非 BitTxn 必须全层下探"规则的来源。

**文件回收**:全部处理完,若 `activeIteratorNum==0`(无活跃事务迭代器在读 vlog)直接从 filesMap 摘除并删文件;否则挂入 `FilesToDel`,等最后一个迭代器 Close 时(`decrIteratorCount` 归零)统一删除。删文件前 `deleteVlogFile` 先持文件写锁(等待正在读的读者释放),unmap、Close、os.Remove。

## 14.5 一致性顺序:vlog 必须先于 WAL

WriteRequest 里 `vlog.Write/Sync` 在 `writeToLSM` 之前:WAL/跳表里的指针一旦持久化,vlog 里对应 value 必然已经持久化。崩溃场景分析:

| 崩溃点 | 结果 |
|---|---|
| vlog 写完、WAL 没写 | value 成为孤儿,重启不被引用,GC 的"LSM 查不到→丢弃"兜底清理 |
| WAL 写完、内存表没刷 | WAL 重放恢复,指针有效,vlog 数据在 |
| 提交返回后 | vlog+WAL 都已落盘(SyncWrites)或同在页缓存(默认),语义一致 |

> 验证:`vlog_test.go`(大 value 读写往返、轮转、GC 后数据仍在、Fid 隔离)、`compaction_gc_test.go` 的压缩→GC 联动。

# 第 15 步 启动恢复与崩溃安全

> 目标:把前 14 步的持久化组件按正确的顺序与规则组装成"任意时刻 kill -9 都能安全重启"。这一章是前面所有"崩溃窗口"讨论的汇总。

## 15.1 启动序列

```
TrainKV.Open
├─ flock 目录锁(防双开)
├─ 初始化 ValueLog 对象与两条统计通道(DiscardStatsCh / TxnDoneIndexCh)
├─ lsm.NewLSM
│   ├─ InitLevelManger:打开 MANIFEST(回放+截尾)→ checkSSTable 对账删孤儿
│   ├─ build:打开全部已登记 .sst → 挂层 → 排序;maxFID=manifest 最大
│   ├─ 起 StartFlushMemTable 协程
│   └─ recovery:扫描 .wal → fid==sstMaxID 的删掉(flush 窗口残留)
│               其余重放(组规则,Truncate 尾部)→ 有数据的成为 immutable 入 flush 队列
│               → 新建空的可写内存表
├─ TransactionManager 播种(nextTxnTs=MaxVersion+1;两 Mark 水位=MaxVersion)
├─ 起 handleWriteCh 消费者 / vlog waitOnGC 门栓
├─ vlog.Open:扫描 .vlog → 打开 → 最后一个文件迭代定位可写 offset
│             → sendDiscardStats 读回上次的 discard 统计
└─ 起 NumCompactors 个压缩协程
```

## 15.2 持久化顺序约束表(全部约束的根)

| 顺序 | 原因 |
|---|---|
| vlog value 先于 WAL 指针落盘 | WAL 恢复出的指针必须能读到完整 value |
| WAL 先于内存表刷成 SST | 内存表丢了可重放;SST 落盘后 WAL 才可删 |
| SST 文件先于 manifest 登记 | manifest 是"哪些表存在"的唯一权威,登记后才算提交 |
| manifest 变更先于内存层更新 | 崩溃后以 manifest 为准对账,内存层可重建 |
| 新表登记先于旧表删除 | 压缩中间态崩溃 → 新旧并存无害,后续压缩自然收敛 |
| 目录 fsync(SyncDir)在文件创建/改名后 | 否则文件目录项可能先于数据丢失 |

## 15.3 各崩溃窗口与恢复路径对照

| # | 崩溃窗口 | 重启后的处理 | 关键机制 |
|---|---|---|---|
| 1 | 事务提交中(组写了一半 WAL) | 半组丢弃(fin 标记之后才生效) | BitTxn…BitFinTxn 组规则 + validEndOffset |
| 2 | vlog 写完,WAL 未写 | value 孤儿,无人引用,GC 兜底清掉 | 先 vlog 后 WAL |
| 3 | WAL 写完未 flush | WAL 重放恢复为 immutable 表,重新 flush | recovery |
| 4 | flush 建好 5.sst,manifest 未登记 | 5.sst 不在 manifest → checkSSTable 删孤儿;5.wal 重放后重新 flush(同 fid) | 对账 + fid 幂等 |
| 5 | manifest 已登记,5.wal 未删 | 5.sst 已恢复;5.wal 因 fid==sstMaxID 被删 | fid 共享命名空间 |
| 6 | 压缩提交中(新旧表并存) | manifest 全量恢复;重启用例靠压缩自然收敛;孤儿(目录有、manifest 无)被删 | AddChanges 先行 |
| 7 | vlog 轮转截断中 | 文件按有效末尾截断;半条记录视为尾部 | DoneWriting + iterator 容错 |
| 8 | vlog GC 重写中 | 重写条目(无 BitTxn)单条原子;未重写的活数据下次 GC 再搬 | 非事务条目单条提交 |
| 9 | discard 统计写入中 | 统计 key 未写或半组丢弃 → 该轮统计丢失,后续压缩重新累积 | 统计可丢可重建 |

## 15.4 孤儿与残留清理规则汇总

| 残留 | 判定 | 处理 |
|---|---|---|
| `.sst` 不在 manifest | checkSSTable | 删除 |
| `.sst` 在 manifest 但文件丢失 | checkSSTable | 报错(拒绝静默丢数据) |
| `.wal` fid == manifest 最大表 id | recovery | 删除(flush 窗口) |
| `.wal` fid 更大 | recovery | 重放 → 重新 flush(转成同名 sst 再登记) |
| `.wal` 尾部半条/零填充 | recovery2SkipList + WAL.Read | 视为尾部,Truncate 裁掉 |
| MANIFEST 尾部半条变更 | ReplyManifestFile 返回 truncOffset | 启动 Truncate |
| 空的最新 vlog(无写入) | vlog.Close | 删除(避免无效空文件堆积) |
| GC 中未删完的 vlog | FilesToDel 挂起 | 最后一个迭代器关闭时删 |

> 验证:`crash_test.go` 是这张表的可执行版本(多个窗口 kill + 重启断言);`vlog_test.go`、`txn_test.go` 补充语义断言。

---

# 第 16 步 从 0 到 1 实施路线图

> 这一章给"照本文档从零实现"的读者一个建议的里程碑顺序。每个里程碑都有明确产出、涉及源码文件与验证手段。建议严格按序推进,每一步都保持可编译、可测试——后面的正确性依赖前面。

## M1 领域模型 + 内存跳表(纯内存 KV)

- **产出**:`model`(Key 编码/Entry/ValueExt/CompareKeyWithTs)、`skl`(arena + 跳表 + 迭代器)、`interfaces`;
- **验证**:key 往返/排序/多版本顺序单测;跳表 Put/Get/删除覆盖/并发 `-race`;全部用 `go test -race`。
- **陷阱**:CompareKeyWithTs 的三分支(同长/前缀/异 key);arena 固定容量先约定好"谁保证不越界"。

## M2 持久化最小环:WAL + 重放

- **产出**:`file/mmap`、`lsm/wal.go`、`MemoryTable` 雏形(单表 Put/Get)、`recovery2SkipList`;
- **验证**:写一批 → 不关文件直接 reopen → 数据一致;手改文件尾部模拟半条记录 → 重启不 panic;`-race`。
- **陷阱**:零填充 vs 真损坏的区分;validEndOffset 必须在"最后一条完整记录"处。

## M3 SSTable:构建 + 读取 + 布隆

- **产出**:`sstBuilder.go`(块切分/前缀压缩/索引/布隆)、`ssTable.go`(Init 表尾解析)、`table.go` 的 Table.Search 前身(块内二分);
- **验证**:构建→OpenTable→逐条比对;篡改块数据/索引 → checksum 报错;大表 maxKey 逆序迭代获取。
- **陷阱**:块尾元数据顺序(offsets→num→chk→chkLen)两端的编解码必须镜像;bloom 的 key 必须去 ts。

## M4 flush + L0 + MANIFEST:可重启的 LSM 雏形

- **产出**:`lsm.go` 轮转/flush 协程、`levelsManger.flush`、`manifest.go`、启动 build/recovery 对账;
- **验证**:写满轮转多次 → 重启 → 全部数据可读;在 flush/manifest 窗口反复 kill(可写个小脚本循环);`crash_test.go` 的思路;
- **陷阱**:wal/sst 同 fid 的删除规则;先登记 manifest 再挂层;flush 空表跳过。

## M5 分层与压缩(核心难点)

- **产出**:`compact.go` 的调度/选表/CAS 区间/执行;LevelHandler 的层管理;版本回收(水位前可先写死"只合并不删版本",再引入 NumVersionsToKeep);
- **验证**:四类任务路径;压缩后读路径正确(新值保留、旧值消失);并发多 compactor `-race`;`compaction_paths_test.go`;
- **陷阱**:区间 CAS 防重叠;同 key 多版本不能跨输出表拆分;表引用计数与文件删除的时机。

## M6 块缓存与读路径整合

- **产出**:`utils/cache`(先做 16 分片 LRU 也行,再升级 SLRU/CMS/门卫)、Table.Search 接入、迭代器接入 IsSetCache;
- **验证**:命中率/淘汰语义单测;读放大 bench;Table 删除后缓存无悬垂(靠 Del 联动)。
- **陷阱**:get 后必须重解析 element(slru 互换技巧);value 是共享块视图,出缓存必须拷贝。

## M7 迭代器与范围扫描

- **产出**:`lsm_iterator.go` 全部四类 + 事务迭代器的非事务版本(可见性过滤可以留到 M9);
- **验证**:跨表/跨层归并序(注释里的 foo/zoo 例子)、Seek 落间隙、降序;`mergeSort_test.go`。

## M8 vlog:大 value 分离 + GC

- **产出**:`vlog.go/vlogFile.go`、ValuePtr 接入写路径、读路径惰性展开、discard 统计、runGC;
- **验证**:大 value 读写往返/轮转/重启;GC 后数据完整;与压缩联动(discard 驱动);`vlog_test.go`。
- **陷阱**:先 vlog 后 WAL 的顺序;GC 期间文件的读锁回调契约;FilesToDel 延迟删除。

## M9 MVCC 事务与水位回收

- **产出**:`limiterMark.go`、`txn.go` 全量、TransactionManager、压缩接 discardTs;
- **验证**:读己之写、冲突 abort、快照隔离(读写并发下读不阻塞写)、版本回收后旧快照语义;`txn_test.go`;
- **陷阱**:读事务等待 commitMark 与写事务延迟等待;GC 重写条目的 BitTxn 缺失对读路径的影响(第 7.5 节);水位播种边界。

## M10 group commit 写通道

- **产出**:`db.go` 的 writeCh/handleWriteCh/WriteRequest、Request 池化与 ref;
- **验证**:并发提交吞吐、SyncWrites 下 fsync 次数≈批次数的压测断言、失败区间标记单测。

## M11 打磨与平台化

- **产出**:mmap 平台差异全量验证(linux 必测;darwin/windows 走 unmap+remap 路径)、`benchmk/` 基准、参数化调优;
- **验证**:`benchmk/bench_*_test.go`、`go test -race ./...` 全绿;对照 `bench_profile/` 与 `docs/*.md` 中记录的问题(写路径优化、读路径优化、性能优化指南)做复盘。

## 贯穿性的工程纪律(本项目注释里反复出现的经验)

1. 热路径禁止无谓分配:二分用手写循环(闭包逃逸)、crc 对象复用、key 缓冲池化、u32 切片零拷贝——profile 驱动,不要预先优化;
2. 所有"两段式"操作(先 A 后 B)必须回答:**A 和 B 之间崩溃了怎么办**,并留下注释;
3. 引用计数 + 通道容量是主要的并发原语,锁越少越好,但"锁内 IncrRef/拷贝切片"这类窗口必须锁着做;
4. 防御断言用 CondPanicf,把不变量写进代码(arena 越界、块越界、水位回退都是 fatal,不要静默);
5. 每个子系统配独立的测试文件,窗口类问题用 crash_test 风格循环验证。

---

# 附录 A 默认参数表

| 参数 | 默认值 | 说明 |
|---|---|---|
| MemTableSize | 64MB | 内存表(arena/WAL 预分配)上限 |
| WaitFlushMemTables | 6 | flush 有界队列容量(背压点) |
| BlockSize | 4KB | SST 数据块目标大小 |
| BloomFalsePositive | 0.01 | 布隆误判率 → bits/key≈10 |
| CacheNums | 10240 | 块缓存条目数(≈40MB 块数据) |
| ValueThreshold | 1MB | 大 value 分离阈值(`maxValueThreshold`) |
| ValueLogMaxEntries | 1000 | vlog 文件最大条数(轮转条件之一) |
| ValueLogFileSize | 1GB−1 | vlog 文件目标大小 |
| ValueLogFileMaxSize | MaxUint32 | 单请求/文件偏移硬上限 |
| SyncWrites | false | 提交是否 fsync(默认靠页缓存) |
| VerifyValueChecksum | false | vlog 读校验 |
| MaxBatchCount / MaxBatchSize | 1000 / 10MB | 批/事务上限 |
| NumCompactors | 4 | 压缩协程数 |
| BaseLevelSize | 10MB | base level 期望大小 |
| LevelSizeMultiplier | 10 | 层间总量 10 倍递减 |
| TableSizeMultiplier | 2 | 深层单表大小 ×2 |
| BaseTableSize | 2MB | base 层单表期望大小 |
| NumLevelZeroTables | 5 | L0 表数阈值(分数=表数/5) |
| MaxLevelNum | 7 | L0–L6 |
| SSTKeyRangCheckNums | 5000 | 压缩重叠预检的 key 间隔 |
| NumVersionsToKeep | 1 | 每 key 保留版本数 |
| DetectConflicts | true | 读写冲突检测开关 |
| KvWriteChCapacity | 1000 | writeCh 容量 |
| WriteChBatchThreshold | 900 | 攒批强刷阈值(<1000) |
| SyncGroupCommitWait | 1ms | SyncWrites 汇合窗口 |
| LevelMaxStaleDataSize | 10MB | 末层 stale 合并阈值 |
| MaxAllocatorInitialSize | 256MB | sstBuilder 扩容上限 |
| ManifestDeletionsRewriteThreshold / Ratio | 10000 / 10 | manifest 重写条件 |
| WaitFlushMemTables 相关 | — | flush 通道容量即背压 |
| MaxKeySize | 65000 | key 上限 |
| MaxHeaderSize | 21 | WAL/vlog 记录头上限 |

# 附录 B 磁盘格式速查

| 文件 | 记录/布局 |
|---|---|
| `%05d.wal` | `[meta 1B][uvarint klen][uvarint vlen][uvarint exp] key value crc32(4B BE)]` 逐条追加;事务组以 BitTxn 起、BitFinTxn 收 |
| `%05d.sst` | `[block0..blockN][TableIndex protobuf][len 4B][chk 8B][len 4B]`;block = `[entry…(overlap:u16,dif:u16,diffKey,meta,varint exp,value)][entryOffsets 4B×N][num 4B][chk 8B][chkLen 4B]` |
| `%05d.vlog` | 记录布局同 wal;文件按 offset/条数轮转 |
| `MANIFEST` | `['MAGC' 4B][version 4B]` + 若干 `[len 4B][crc 4B][ManifestChangeSet protobuf]`;重写时经 REWRITEMANIFEST rename |
| 数值 | 文件尾标量 4/8B 大端;块内 entryOffsets 原生小端零拷贝;ValuePtr 12B 原生布局 |

# 附录 C 并发与锁使用速查

| 对象 | 保护方式 |
|---|---|
| LSM.memoryTable / immemoryTables | LSM.RWMutex(读路径锁内取引用并 IncrRef) |
| 跳表 | 无锁(arena 原子分配 + CAS 层链) |
| LevelHandler.tables/总量 | 层 RWMutex;表引用计数管生命周期 |
| compactIngStatus | 专用 mutex + 区间 CAS |
| LevelManger.maxFID / txnDoneIndex | atomic |
| WAL.writeAt / VLogFile.size | atomic(写被 writeCh 串行化) |
| vlog.filesMap | filesLock RWMutex;单文件 mmap 访问走 VLogFile.Lock(读锁回调) |
| Block 缓存分片 | 分片内写锁(Set/Get 都持写锁,16 片降竞争) |
| MANIFEST | ManifestFile.mux + 每次 f.Sync |
| Table.ref / SkipList.ref | atomic.Int32,归零触发清理/删除 |
| TransactionManager | tsLock(时间戳)、writeChLock(提交临界区)、LimitMark 单消费协程 |
| flush/GC/compactor | Closer 信号 + WaitGroup;flushMemTable 有界通道背压;GarbageCh 门栓 |

# 附录 D 术语表

| 术语 | 含义 |
|---|---|
| keyTs | 带 8B ts 的内部 key |
| memtable / immutable | 可写内存表 / 已轮转等待 flush 的内存表 |
| SST / Table | 磁盘有序表文件 / 逻辑表对象 |
| Block / restart point | 4KB 数据块 / entryOffsets 数组 |
| flush | immutable 落成 L0 SST |
| compaction | 层间归并、版本回收、stale 清理 |
| base level | L0 的直接目标层 |
| discard stats / discardTs | 死数据统计 / 读事务水位 |
| stale data | 墓碑等必须保留的失效数据 |
| vlog / ValuePtr | value 日志 / 12B 定位指针 |
| startTs / commitTs | 快照时间戳 / 提交时间戳 |
| W-TinyLFU | 窗口 LRU + 分段 LRU + 门卫 + CM-Sketch 缓存算法 |
| BitTxn / BitFinTxn | WAL 事务组起止标记 |

# 附录 E 常见设计问题 FAQ

1. **为什么同 userKey 的多版本能靠 bytes.Compare 排对序?** 因为 ts 取反存储:版本越大编码越小,同 key 内"新在前";层间排序、块内二分、归并堆全部复用同一个比较函数。
2. **跳表一个 key 能存多个版本,为什么 Get 不混淆?** Get 按 keyTs 找"最近 ≥ 的节点",SameKeyNoTs 复核后**显式解析节点版本号**返回,由上层(LSM.Get)做可见性判断,不是"谁在跳表里就返回谁"。
3. **arena 固定容量写满会怎样?** 不会写满——LSM.Put 先按上界估算,超限先轮转;真越界是 AssertTrue fatal(内部错误,不静默)。
4. **为什么轮转按 arena 用量而不是 WAL 大小?** arena 不可扩容且与 WAL 消费速率不同步,WAL 大小不能约束 arena 用量(旧逻辑的教训,见代码注释)。
5. **WAL 与 SST 为什么共享 fid?** flush 是"同名转换",崩溃窗口的残留可用 fid 幂等清理;vlog 独立 fid 是因为它不与任何 LSM 对象一一对应。
6. **L0 为什么按 maxVersion 降序而不是按创建序/fid?** L0 读路径"新表在前可早停",版本序才是真正的数据新旧序(fid 序会被并发 flush 打乱)。
7. **压缩产物为什么不喂块缓存?** 压缩读是顺序全量扫描,喂缓存只会把热点块挤出去。
8. **压缩输出为什么按水位提前快照 discards?** 任务可能跑数秒,水位中途推进会改变"哪些版本可删"的判定,同 key 前后版本按不同水位判定会删错;快照保证整任务一致。
9. **墓碑什么时候被物理清除?** 压缩时:墓碑版本 ≤ 水位、无更深层重叠、非末层 → 直接丢;末层墓碑转 stale,等表 stale ≥10MB 且表 MaxVersion ≤ 水位且创建超 1h 的 Lmax→Lmax 压缩。
10. **为什么末层墓碑要等这么久才清?** 水位保证"没有活跃读者",1h 保证"刚生成的表不反复合并",10MB 保证"合并收益够大"。三项都是防抖动。
11. **写事务为什么要等 commitMark?读事务为什么不等?** 读事务要立刻保证快照完整(所有 ≤ startTs 的提交可见);写事务若从不读 LSM(纯写),这笔等待是纯开销,延迟到首次读才付。
12. **冲突检测为什么用双哈希?** 单 64 位哈希有碰撞误报风险;MemHash+xxhash 双登记后,只有两个都撞才会误报。
13. **为什么 vlog 记录要存完整 key 而不只是 ValuePtr?** GC 需要拿记录的 key 去 LSM 反查当前版本做去留判定;重写也要原 key。
14. **ValuePtr 12B 存了什么?** Fid/Offset/Len 各 4B;Len 含记录头与 crc(整条记录长度),读时一次切出。
15. **SyncWrites=false 会丢数据吗?** 默认提交不 fsync:进程崩溃不丢(数据在页缓存,WAL 重放可恢复);操作系统崩溃/断电会丢最近未回写窗口。SyncWrites=true 每个提交组 fsync 一次(配合 1ms 汇合窗口摊薄)。
16. **大 value 读为什么返回回调而不是自动解锁?** mmap 视图的生命周期必须覆盖到调用方拷贝完成;回调让"锁的持有"显式化,GC 轮转删除文件时才能安全等待。
17. **为什么用 mmap 还要 SyncDir?** 文件创建/改名是目录元数据变更,不 fsync 目录,数据落盘了目录项可能还没落,断电后文件"消失"。
18. **Windows 上 SyncDir 为什么是空操作?** Windows 不允许对目录句柄 fsync(Badger#699),只能接受该平台文件系统对目录更新的处理。
19. **为什么 WAL/vlog 记录头用 varint 而 block 条目头用定长?** WAL/vlog 是顺序写顺序读,varint 省空间;block 条目头需要随机二分定位,定长 4B 便于偏移计算。
20. **file 包与 utils 包各有一份 SyncDir 会冲突吗?** utils 版按平台分化(Windows no-op);file 版仅 OpenMmapFile 新建文件时用。属轻微重复,可合并(见代码注释)。
21. **为什么块缓存淘汰表文件时要逐个 Del 自己的块?** 文件删除后映射失效,缓存里残留块会成为悬垂内存视图;Table.ref 归零点做全量 Del。
22. **恢复时 wal fid 等于 sstMaxID 为什么直接删而不是重放?** 该 wal 的内容必然已经 flush 成同名 sst 且登记成功(否则 fid 不会出现在 manifest 里);重放会造成数据重复。
23. **为什么压缩把"同 key 版本决策"限制在单输出表内?** skipKey/保留判定必须在看到该 key 全部版本后才能定论,跨表拆分会让同一个 key 的前半段按旧水位、后半段按新水位处理。
24. **为什么 discard 统计 key 用 MaxUint64 版本?** 该 key 是内部元数据,不能出现在任何用户快照里;MaxUint64 版本只有"精确按此 ts 查"才能读到。

# 附录 F 本仓库配套资料

- 文档:`docs/write-path-optimization.md`(写路径优化过程)、`docs/read-path-optimization.md`(读路径优化)、`docs/performance-optimization-guide.md`、`docs/improvements-summary.md`/`improvements-detail.md`(问题清单)、`docs/comparison.md`(与 Badger 对比)、`docs/todo-list.md`(待办)、`docs/interview-prep.md`/`interview-qa.md`/`resume.md`(面试向梳理)
- 测试:各组件同名 `*_test.go` + `crash_test.go`(崩溃窗口)、`compaction_gc_test.go`(压缩与 vlog GC 联动)、`compaction_paths_test.go`(压缩路径)
- 基准与剖析:`benchmk/`(bench 数据集与基准)、`bench_profile/`(pprof 产物)、`docs/go_test_bench.txt`(基准数据)
- 可运行示例:`example/main.go`
