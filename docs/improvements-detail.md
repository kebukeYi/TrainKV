# TrainKV 改进总结(详细技术版)

> 面向需要代码级细节的读者。通俗版见 `improvements-summary.md`。
> 涉及文件:db.go、txn.go、vlog.go、vlogFile.go、lsm/*、skl/*、model/*、interfaces/*、utils/cache/*、benchmk/*、compaction_gc_test.go、compaction_paths_test.go。

---

## 一、写路径性能优化(两轮,8 项)

### 第 1 轮:事务与写通道

**① handleWriteCh 内联写**(db.go)
- 现象:每攒满一批就 `go writeRequest(reqs)` 起写协程,串行提交下每 op 一个 ~2KB 栈的协程;批次切片每次 `make`。
- 改法:令牌(`blockChan`)空闲时在当前协程直接写盘,仅忙时才派协程;`reqs[:0]` 复用底层数组。
- 收益:消灭每批协程创建 + 切片分配。

**② 事务池化**(txn.go、db.go)
- 现象:`NewTransaction` 每事务 3 次分配(结构体 + `pendingKeys` map + `conflictKeys` map),占 pprof 34.6%。
- 改法:`db.txnPool` 复用 `Transaction`;`Discard`(在 `req.Wait()` 之后执行)clear map 后归还。
- 契约变化:Commit/Discard 后不得再使用事务(与 Badger 一致);依赖旧语义的测试 bug 被暴露并修复。

**③ finTxn 标记 entry + ts 缓冲复用**(txn.go)
- 现象:每提交 `model.NewEntry(txnKeyBytes, strconv.AppendUint(nil, ...))` = Entry 结构体 + ts 字符串 2 次分配。
- 改法:池化事务内嵌 `finEntry` 对象 + `finTsBuf`(`finTsBuf[:0]` 复用)。

**④ conflictKeys 快照化**(txn.go)
- 现象:`conflictKeys` 被 `commitedTxns` 以 map 引用持有,无法池化;每事务新建 map + 首次插入扩容分配。
- 改法:提交时把写 key 哈希拷贝成 `[]uint64` 快照存入 `commitedTxns`;`hasConflict` 改线性扫描;map 归还池复用。
- 前提关系:④ 是 ② 能安全池化 `conflictKeys` 的前提。

### 第 2 轮:vlog 大 value 路径

**⑤ EncodeEntry 单次 CRC**(vlogFile.go)
- 现象:`crc32.New` 的 `&crc32.digest` 逃逸;`io.MultiWriter` 接口调用使 `headerBuf`/`crcBuf` 逃逸。
- 改法:先编码进 `vlog.buf`,再 `crc32.Checksum(out.Bytes()[start:], table)` 一次性计算(与 WAL 侧对齐)。

**⑥ ValPtr 值切片**(model/Request.go、vlog.go)
- 现象:`append(req.ValPtr, &p)` 使局部 `p` 逃逸堆分配。
- 改法:`[]*ValuePtr` → `[]ValuePtr`;LSM 直写条目以零值占位。

**⑦ 请求级 ValuePtr 编码缓冲**(db.go、model/Request.go)
- 现象:`ValuePtr.Encode()` 每次 `make([]byte, 12)`。
- 改法:`req.EncodeValPtr(p)` 写入请求私有 scratch(首次增长后摊销为 0),`lsm.Put` 同步消费。

**⑧ KeyWithTs key-buffer 池**(model/Key.go、txn.go)
- 现象:提交路径每 entry 一次 key+8 拷贝。
- 改法:`KeyTsBufPool` 池化 `*[]byte`(指针装箱零分配,缓冲在盒内增长)。
- **踩坑记录**:第一版池化 `[]byte` 时,`sync.Pool.Put` 的 interface 装箱(24B slice header 放不进单字 data 槽)每次分配,净亏损(profile 显示 `Discard` 内 43,691 次分配);改池化指针后修复。

## 二、读路径性能优化(3 方向)

**方向 1:合并层去拷贝 + memtable 稳定 Item**
- `MergingIterator.Item()` 直接返回子迭代器 Item(删除每 key 2 次 `SafeCopy`);
- 新增 `skl.ChunkedArena`(64KB 分块,块不移动,返回切片永久有效);
- `SkipListIterator` 增加 `copyItems` 模式:活跃 memtable 用副本(防并发写 arena 扩容悬垂),immutable/flush 用视图。

**方向 2:SST 扫描 key 拷贝 arena 化**
- `blockIterator.setIndex` 的 key 拷贝改走 `ChunkedArena`(TableIterator 注入)。

**方向 3:大 value 惰性读**
- `interfaces.Item` 增加 `VP`/`Vlog` 字段与 `Value()` 方法(`model.ValueReader` 由 `*ValueLog` 实现);
- `TxnIterator.Item()` 对 `BitValuePointer` 条目只记录 12B 位置,不再立即拷贝;
- **契约变化**:Item 为借用语义(`Rewind` 后旧 Item 失效,arena 重置);大 value 取值改用 `item.Value()`。

## 三、正确性修复(14 项)

### 读路径

1. **block 缓存幽灵 key**(utils/cache/cache.go)
   - 根因:W-TinyLFU 的 `segmentedLRU.get` 晋升时 `*bitem, *item = *item, *bitem` 交换两个 `list.Element` 的 storeItem 内容,key→element 映射改变;`Cache.get` 返回交换前的旧 element → key A 读到 key B 的 block。
   - 现象:flush 后读回 ~1.4% key 静默丢失,每次扫描丢失的 key 不同;SST 文件本身完全正确。
   - 修复:`Cache.get` 在 stage 操作后重新解析 `element, ok = c.data[keyToHash]`。
   - 回归测试:`benchmk/flush_integrity_test.go`(100K keys 读回 0 丢失)。

2. **MergingIterator 重扫 bug**(lsm/lsm_iterator.go)
   - 根因:`Rewind`/`Seek`/`Close` 遍历的是扫描中不断收缩的堆,耗尽后变空 → 二次扫描返回 0 条、Close 不关子迭代器。
   - 修复:保留原始 `iters` 集合,`Rewind`/`Seek` 基于它重建堆,`Close` 关闭全部子迭代器。

3. **Table.DecrRef 非原子读**(lsm/table.go)
   - 根因:`atomic.AddInt32` 之后用普通读 `t.ref == 0` 判断归零;split 并行压缩并发 DecrRef 时竞争,可能提前删除仍被引用的表。
   - 修复:用 `AddInt32` 返回值判断。

### 生命周期/并发

4. **getTxnDoneIndexFromCh 协程未等待**(lsm/levelsManger.go)
   - 根因:`close(stopCh)` 只发信号不等待;复用 `opt` 重开 DB 时,旧协程仍读 `opt.TxnDoneIndexCh` 与新 `Open` 的写竞争。
   - 修复:`stopWG.Wait()`。

5. **discardStats 发送协程泄漏 + nil 通道无防护**(lsm/compact.go)
   - 根因:compaction 派发的统计发送协程从不等待;`DiscardStatsCh` 为 nil 时(独立 LSM 场景)永久阻塞泄漏。
   - 修复:`discardStatsWG` 纳入等待 + close 等待 + nil 防护。

6. **GetTables() 无锁**(lsm/levelsManger.go)
   - 诊断访问器不加锁返回 `tables`,与 flush/compaction 并发写竞争。
   - 修复:RLock。

7. **addSize 无锁**(lsm/levelHandler.go)
   - `addSize` 无锁而 `subtractSize` 持锁,flush 与 compaction 的 size 统计竞争。
   - 修复:两函数加锁 + `addSizeLocked`/`subtractSizeLocked` 内部版(供已持锁的 updateTable/deleteTable 使用,避免自死锁)。

8. **vlog.maxFid 原子/非原子混用**(vlog.go)
   - 读路径(`getVlogFileLocked`)普通读与写侧轮转(`createVlogFile` 普通写/原子 Add)竞争。
   - 修复:改 `atomic.Uint32`,全部访问点统一 Load/Store/Add。

### flush/GC

9. **Rotate 修复条件反转**(lsm/lsm.go,你手动修复中的问题)
   - 根因:`if im == nil || im.Size() > 0 { return }` 应为 `== 0`——有数据的 memtable 永不 flush,数据到不了 SST。
   - 修复:反转条件;由 `TestValueLogGCFull` 的调试过程抓出。

10. **compactBuildTables 用目标层号判断分支**(lsm/compact.go)
    - 根因:switch 以 `level`(目标层)判断 L0 分支;L0→dst(如 L0→L6,level=6)选中多张重叠 L0 表时落入"非 L0 源"分支 → `log.Fatalf` 杀进程。
    - 修复:改按 `cd.thisLevel.levelID == 0`(源层)判断。

11. **gcReWriteLog 查不到旧版本即中止**(vlog.go)
    - 根因:压缩清掉被覆盖的旧版本(`NumVersionsToKeep=1`)后,GC 用旧 key+ts 查 LSM 得到 not-found,直接返回错误中止整次 GC。
    - 修复:not-found ⇒ 该 vlog 条目已死,丢弃。

### 压缩路径死代码(4 处)

12. **L0→L0 永不被调度**(lsm/compact.go)
    - 根因:`pickCompactLevels` 的 `out` 过滤只保留 `score >= 1.0` 的层;L0 表数 <5(正是 L0→L0 场景)被完全排除。
    - 修复:L0 层总是参与调度,由 `findTablesL0ToL0` 内部 ≥4 张把关。

13. **StaleDataSize 从未持久化**(lsm/sstBuilder.go)
    - 根因:`buildBlockIndex` 未把 `ssb.staleDataSize` 写入 `TableIndex.StaleDataSize` → 所有表 stale 恒为 0 → `findMaxLevelTables` 第一步即返回 false。
    - 修复:索引写入 `StaleDataSize`。

14. **删除标记进不了末层**(lsm/compact.go)
    - 根因:标记保留条件 `hasOverlap` 对 `nextNext ≥ MaxLevelNum` 恒 false → 进入 L6 的压缩都丢弃标记 → 末层无 stale 可回收。
    - 修复:`keepForLastLevel := cd.nextLevel.isLastLevel() && expired`,末层保留标记,由 Lmax→Lmax 统一回收。

15. **findMaxLevelTables 大表选中却不压缩**(lsm/compact.go)
    - 根因:`t.Size() >= needFileSize` 时 `break` 落到 `return false`(注释"找到就返回"与实现矛盾)。
    - 修复:大表登记压缩状态后直接 `return true`。

## 四、测试建设

| 测试 | 位置 | 覆盖 |
|---|---|---|
| `TestFlushReadIntegrity` | benchmk/flush_integrity_test.go | 幽灵 key 回归:flush 后 100K 读回 0 丢失 |
| `TestDBAutoCompaction` | compaction_gc_test.go | 60K 写+更新+删除 → 5 L0 表 → RunOnce → 全量读回 + 压缩期间并发读 |
| `TestValueLogGCFull` | compaction_gc_test.go | GC 全链路:统计 → 挑选 → 重写 → 删旧文件 → 重启 → 并发读 |
| `TestCompactionL0ToL0` | compaction_paths_test.go | L0 小表自合并(4→1 表) |
| `TestCompactionL0ToLmax` | compaction_paths_test.go | L0 直达末层(5 表 → L6) |
| `TestLastLevelCompaction` | lsm/last_level_compact_test.go | 末层 stale 合并(45 万标记,10MB stale) |
| `-race` 全量 | 全部包 | 并发安全 |

基准新增(benchmk/bench_perf_test.go):读命中/未命中/随机、SST 扫描、大 value 惰性扫描、混合 9:1。

## 五、基准数据(count=3 中位数)

| 基准 | 指标 | 优化前 | 优化后 |
|---|---|---|---|
| TrainKVTxnSet | allocs/op | 25 | 14(-44%) |
| TrainKVTxnSetParallel | allocs/op | 18 | 8(-56%) |
| TrainKVBatchSet10 | allocs/op | 84 | 64(-24%) |
| TrainKVTxnSetBigValue | allocs/op | 14 | 8(-43%) |
| ReadGetHitSeq(memtable) | allocs/op / ns | 2 / ~1.2µs | 2 / ~1.2µs(无回退) |
| ReadIterate(memtable 100K) | allocs/op | 200,000 | 832(-99.6%) |
| ReadIterateSST(100K) | allocs/op / B/op | —(此前不可测) | 0 / 0 |
| ReadIterateBigValue(500×1MB 惰性) | B/op / ns | —(旧实现 528MB/次) | 65KB / ~110µs |

注:写路径 ns/op 受 fsync 主导(WSL2 噪声大),allocs/op 是稳定指标。
