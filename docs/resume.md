# TrainKV 简历项目描述(定稿)

> 三种篇幅:完整版(简历空间充足)、紧凑版(标准简历)、一句话版(项目列表)。
> 所有数字都可在本仓库复现:基准见 `docs/improvements-detail.md` 第五节,缺陷清单见第三节,
> 面试追问见 `docs/interview-qa.md`。

---

## 完整版

**TrainKV — 纯 Go 实现的 LSM-Tree 键值存储引擎**(个人开源项目 | Go | 核心约 1 万行,无 CGO)

参照 Badger 架构从零实现的嵌入式 KV 存储引擎,覆盖 LSM 全栈:MVCC 事务、WAL 崩溃恢复、
分层压缩、块缓存、value 日志分离与垃圾回收,五条核心链路均有端到端测试并以 `-race` 验证。

- **存储架构**:跳表 memtable(arena 分配器)+ WAL → L0–L6 分层 SSTable(三条压缩路径);
  kv 分离——超过 1MB 的 value 写入追加式 vlog,LSM 仅存 12 字节指针;MANIFEST 表登记 +
  WAL 重放保证崩溃安全;并发写经 group commit 攒批摊销 fsync。
- **事务**:TSO 时间戳的 MVCC 读写事务,支持双哈希冲突检测(乐观并发)与读己之写;
  以水位标记(LimitMark)跟踪活跃读者,驱动压缩时旧版本的安全回收。
- **读路径**:W-TinyLFU 块缓存(16 分片 + CM-Sketch 频率统计 + doorkeeper 布隆准入)+
  SST 布隆过滤器剪枝,memtable 命中读 ~1.2µs/op(约 80 万 ops/s);迭代器扫描 10 万条的
  内存分配从 20 万次降至 832 次(-99.6%),SST 扫描零分配;大 value 惰性读取,
  500×1MB 扫描的数据搬运量从 528MB 降至 65KB。
- **写路径**:事务池化、提交标记复用、vlog 单次 CRC、key 缓冲池等 8 项分配优化:
  单条写 25 → 14 allocs/op(-44%),并发写 18 → 8(-56%),1MB 大 value 写 14 → 8;
  关闭 SyncWrites 后单条写 ~3.2µs/op。
- **正确性**:以端到端测试 + `-race` 为纪律,排查并修复 15+ 深层缺陷——含 W-TinyLFU
  热点晋升交换导致 1.4% key 静默丢失的缓存映射错误、三条压缩路径永不触发的死代码
  (调度过滤、StaleDataSize 未持久化、删除标记不进末层)、Table 引用计数非原子读等;
  读 / 写 / 压缩 / 重启恢复 / vlog GC 五链路端到端测试全绿。

## 紧凑版(标准简历,4 行)

**TrainKV — 纯 Go 从零实现的 LSM-Tree 键值存储引擎**(约 1 万行,无 CGO)

- 跳表 memtable + WAL、L0–L6 分层压缩(三条压缩路径)、kv 分离(vlog + 12B 指针)、
  MANIFEST 崩溃恢复、group commit;TSO 时间戳 MVCC 事务,双哈希冲突检测。
- W-TinyLFU 块缓存(16 分片 + CM-Sketch + 布隆准入),memtable 命中读 ~1.2µs/op;
  扫描 10 万条分配 20 万次 → 832 次,SST 扫描零分配;大 value 惰性读(528MB → 65KB)。
- 8 项写路径分配优化:单条写 25→14 allocs/op,并发写 18→8(-56%)。
- 修复 15+ 深层缺陷(含缓存晋升交换致 1.4% key 丢失、三条压缩路径死代码);
  读/写/压缩/重启/GC 五链路端到端测试 + `-race` 全绿。

## 一句话版

**TrainKV**:参照 Badger 用 Go 从零实现的 LSM-KV 存储引擎(约 1 万行)——MVCC 事务、
分层压缩、W-TinyLFU 块缓存、kv 分离与 GC、崩溃恢复;写路径 8 allocs/op,扫描零分配,
五链路端到端测试 + race 全绿。

---

## 使用注意

1. **每个数字都能被追问到底**,对应关系:
   - 1.2µs / 832 / 528MB→65KB → `benchmk/bench_perf_test.go`,数据在 README「性能」节;
   - 25→14 / 18→8 / 8 项优化 → `docs/improvements-detail.md` 第一、二节;
   - 1.4% key 丢失 / 三条压缩路径死代码 / 15+ 缺陷 → `docs/improvements-detail.md` 第三节;
   - 五链路测试 → `TestFlushReadIntegrity`、`TestDBAutoCompaction`、`TestValueLogGCFull`、
     `TestCompactionL0ToL0/L0ToLmax`、`TestLastLevelCompaction`、`TestCrashRecovery`。
2. **不要写**「生产可用」「媲美 RocksDB」——项目无生产部署、无数据压缩、压缩调度是
   50s ticker(`docs/comparison.md` 有客观短板清单),面试被戳穿反而失分;
   主动把短板挂出来(见 interview-qa.md 最后一节)是这套描述的设计意图。
3. 写路径 ns/op(~4ms)由 fsync 主导、WSL2 噪声大,简历只用 allocs/op 和关闭 sync 的
   ns/op,这两个指标稳定且可复现。
