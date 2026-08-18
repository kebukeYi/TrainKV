# TrainKV 写流程优化报告:从 pprof 到经验

> 两轮优化,8 项改动,累计把事务写路径的分配从 25 次/op 压到 14 次/op(大 value 路径 14→8),并踩出 3 个值得记住的坑。
> 本文按"为什么这么做"展开,最后浓缩成经验清单。
> 配套改动:db.go、txn.go、vlog.go、vlogFile.go、model/Key.go、model/Request.go、db_test.go(修测试)、vlog_test.go(适配)、benchmk_test.go(新增基准)。

## 一、全景:两轮做了什么,结果如何

```
第 0 轮基线(工作区):TxnSet 25 allocs/op,Parallel 18,Batch10 84
第 1 轮(事务/写通道):TxnSet 25→15,Parallel 18→9,Batch10 84→74,总分配 110,601→24,026(-78%)
第 2 轮(vlog/大 value):TxnSet 15→14,Parallel 9→8,Batch10 74→64,新增大 value 基准 14→8
```

| 基准 | 基线 | 最终 | 降幅 |
|---|---|---|---|
| TrainKVTxnSet(串行事务写) | 25 | 14 | -44% |
| TrainKVTxnSetParallel(并发写) | 18 | 8 | -56% |
| TrainKVBatchSet10(批 10 条) | 84 | 64 | -24% |
| TrainKVTxnSetBigValue(1MB value,新增) | 14 | 8 | -43% |

## 二、方法:`alloc_objects` 怎么用,局限在哪

**用法**:`go test -bench=... -memprofile=x.mem`,然后 `go tool pprof -top -alloc_objects`。看"分配次数"而不是"分配字节"——写路径的痛点是**次数**(GC 压力和指令开销),不是字节。

**三个必须知道的局限**:

1. **小对象被系统性低估**。pprof 按 512KB 采样,32B 的 `crc32.digest`、24B 的 slice header 这类对象几乎采不到。第 2 轮验证时,`EncodeEntry` 里明摆着每次 `crc32.New` 分配,但 pprof top 里完全看不见。**对策:小对象用 `-benchmem` 的 allocs/op 计数 + `-gcflags='-m'` 逃逸分析来验证,别只信 pprof。**
2. **热点会漂移**。优化掉大头后,一次性开销(4 个 compactor 协程栈 16,384、protobuf 包初始化)会升到榜首,看起来像"新热点"。判断标准:看它是不是 per-op(除以 b.N 是否≈每 op 1 次),以及 flat 归属。
3. **pprof 只告诉你"在哪分配",不告诉你"为什么"**。"为什么"要靠逃逸分析(`-m`)和代码所有权分析(谁持有、谁释放)。

## 三、逐项优化:为什么这么做

### 第 1 轮:事务与写通道(pprof 前四大热点)

**① handleWriteCh 内联写——消灭"每批一个 goroutine"**

- 现象:29.6% 的对象数经 `LimitMark.processOn` 归属,实际是 `go writeRequest(reqs)` 的协程栈。
- 为什么是浪费:串行提交下,每批 1 个请求,写协程和收集协程没有任何并发可言;每次 `go` 要分配 ~2KB 栈 + 闭包,还要两次调度。
- 改法:令牌(`blockChan`)空闲时**直接在当前协程写**,只有令牌被占用(上一批仍在写,真的需要并行)才派协程;批次切片 `reqs[:0]` 复用底层数组。
- 经验:**"并发"是花钱买的,串行场景下每批一个协程是纯开销。用令牌 + 非阻塞获取区分"必须并行"和"其实可以串行"。**

**② 事务池化——消灭每事务 3 次分配(本轮最大收益)**

- 现象:34.6% 的对象数来自 `NewTransaction`:结构体(72B)+ `pendingKeys` map + `conflictKeys` map。
- 为什么能池化:事务生命周期清晰——`Commit` 的 `defer Discard()` 在 `req.Wait()`(写请求落盘)之后执行,此时事务的私有没有任何外部引用,可以整体回收。
- 关键前置:**`conflictKeys` 被 `commitedTxns` 以 map 引用持有,直接 clear 会清空别人的冲突记录**。所以先做了 ④ 快照化,才敢池化。
- 契约代价:池化后"Commit/Discard 后不得再使用事务"从软约束变成硬约束(旧指针可能别名到新事务)——与 Badger 一致。
- 经验:**复用对象前先画所有权图:谁在事务结束后还持有它的内部?被外部以引用持有的结构必须先"断引用"(快照/拷贝)才能池化。**

**③ finTxn 标记 entry + ts 缓冲复用——消灭每提交 2 次分配**

- 现象:每提交一个 `model.NewEntry(txnKeyBytes, strconv.AppendUint(nil, ...))` = Entry 结构体 + ts 字符串 2 次分配。
- 为什么能复用:finTxn 条目是内部构造的、每事务恰好一个、值(commitTs 数字)不依赖用户数据。
- 改法:池化事务内嵌 `finEntry` 对象 + `finTsBuf`,提交时 `finTsBuf[:0]` 复用、`AppendUint` 写入复用缓冲。
- 经验:**内部合成的固定结构(标记条目、序号、头)永远可以复用——它没有用户所有权问题。**

**④ conflictKeys 快照化——让 ② 成为可能**

- 现象:`modify` 里新 map 首次插入的 bucket 扩容分配。
- 改法:提交时把写 key 哈希拷贝成 `[]uint64` 快照存入 `commitedTxns`(每提交 1 次小分配,写集合通常很小),`hasConflict` 改为对快照线性扫描;事务的 map 归还池复用。
- 为什么线性扫描可以接受:冲突检测只在"后提交的事务带 readKeys"时触发,纯写 workload 甚至不触发;写集合小,扫描代价低于 map 分配。
- 经验:**数据结构的"引用生命周期"决定了能否复用;用 1 次小额拷贝换取每事务 3 次分配的消除,是划算的。**

### 第 2 轮:vlog 大 value 路径(之前基准没覆盖)

**A1. EncodeEntry 单次 CRC**

- 现象(逃逸分析):`crc32.New` 的 `&crc32.digest escapes`;`io.MultiWriter` 是接口调用,`headerBuf`/`crcBuf` 跟着逃逸。
- 为什么:任何经 interface 的调用都让参数逃逸;hash.Hash 逐段 Write 本身就是为"流式"设计的,而这里数据已全在 `vlog.buf` 里。
- 改法:先编码,再 `crc32.Checksum(out.Bytes()[start:], table)` 一次性算——WAL 侧早已这么做了。
- 经验:**编码类热路径:数据已在手,就用单次函数调用替代接口流式写入。**

**A2. `ValPtr []*ValuePtr` → `[]ValuePtr`**

- 现象:`append(req.ValPtr, &p)` 使局部 `p` 逃逸堆分配。
- 为什么:取地址放进堆上的切片,编译器只能让它逃逸。
- 改法:值切片,`append(req.ValPtr, p)` 拷贝 12B;LSM 直写条目用零值占位(反正没人读)。
- 经验:**结构体只有 12B 时,存值不存指针——"逃逸"的根源是取地址,不取就不逃。**

**A3. ValuePtr 编码改请求级复用缓冲**

- 现象:`ValuePtr.Encode()` 每次 `make([]byte, 12)`。
- 为什么能复用:`writeToLSM` 里 `lsm.Put(entry)` 是同步的(WAL 拷贝、arena 拷贝都在调用内完成),entry.Value 用完即死——所以一个请求级 scratch 可以给整批条目轮流用。
- 改法:`req.EncodeValPtr(p)` 写入请求私有缓冲(仅首次增长分配 1 次,之后摊销为 0)。
- 经验:**"同步消费"是复用的通行证:消费点在调用栈内,缓冲就可以循环用。**

**A4. KeyWithTs 提交拷贝池化(踩了坑,见下)**

## 四、踩坑记录(最有学习价值的部分)

**坑 1:`sync.Pool` 存 `[]byte` 是负优化**

第一版 `KeyWithTsPooled` 用 `sync.Pool` 直接存 `[]byte`。profile 显示 `Discard` 一行 43,691 次分配(每 op 约 50 次)——比不池化还差。

根因:`sync.Pool.Put(x interface{})` 要把 `[]byte`(ptr+len+cap 共 24B)装箱进 interface。interface 只有 1 个字的 data 槽,装不下 3 个字的 slice header,于是**每次 Put/Get 都分配一个堆上的 header**——把 1 次 `make` 换成了每次往返的装箱分配。

修复:池化 `*[]byte`。指针是 1 个字,装箱零分配;而且缓冲在盒内增长,容量不足时不会退化成"每次重建"。

> 通用规则:**sync.Pool 只放"1 个字"的东西(指针、uint64),别放 slice、string、map 这类多字头对象。** `Put([]byte)` 每次装箱分配,`Put(&buf)` 零分配。

**坑 2:pprof 采样对小对象失明(见"方法"一节)**

教训:优化对象数时,**以 `-benchmem` 的 allocs/op 为验收指标,pprof 负责指路、逃逸分析负责取证**。三者配合,缺一不可。

**坑 3:基准数字会骗人**

- B/op 被 64MB arena 预分配的摊销污染:b.N=450 时 64MB/450≈142KB/op,基准前后 b.N 不同,B/op 就不可比。
- ns/op 被 fsync 主导:WSL2 上同机同参数从 2.6ms 波动到 38ms;allocs/op 却是稳定的。
- 对策:**count=3 取中位数;跨 run 只比 allocs/op 和趋势明确的指标;报告里如实标注噪声。**

**坑 4:优化暴露了测试里的隐藏 bug**

`TestDBBatchOperations` 在循环里对已提交的事务再次 `txn.Discard()`——以前靠"对象永不回收"侥幸通过;池化后旧指针别名到新读事务,立刻崩溃。这是**好事**:契约被严格执行时,依赖巧合的代码会现形。修测试本身(改成 Discard 读事务)也是清理。

## 五、浓缩经验(建议背下来)

**方法论**
1. 定位分配热点看 `alloc_objects`,但**小对象要用 `-benchmem` allocs/op + 逃逸分析验证**——pprof 对小对象有采样偏差。
2. 优化后热点会漂移,先除 per-op 再谈一次性开销;per-op 的判断 = flat 次数 ÷ b.N。
3. 基准先问"覆盖了哪条路径":小 value 基准不碰 vlog 编码,大 value 路径的坑要靠新基准暴露。
4. 基准必须同条件、多次、取中位数;fsync 场景下 allocs 才是稳定指标。

**Go 机制**
5. 逃逸分析是设计工具:`-gcflags='-m'` 提前暴露热路径逃逸,比事后 profile 早一轮发现。
6. 接口调用是逃逸放大器:具体类型调用、单次函数替代流式接口、池化指针,三招堵漏。
7. **sync.Pool 只存指针**;`Put([]byte)` 装箱分配是经典陷阱。
8. goroutine 是 2KB 起步的分配:令牌空时内联,忙时才 `go`。
9. map 不免费:每 map 一次分配 + 扩容分配;复用前先解决"谁还持有引用"(快照化)。
10. 12B 的结构体用值切片 + 请求级 scratch,零分配。

**工程**
11. 复用的归还点选在**同步屏障之后**(req.Wait → Discard),安全边界清晰。
12. 池化 = 契约变更:文档写明"用后不可再持",测试里依赖旧行为的代码必须修——这是清理,不是妥协。
13. 每次优化问三个问题:谁持有它?什么时候死?同步消费还是异步消费?答清楚再动手。

## 六、数据附录(全部实测,count=3)

| 基准 | 指标 | 基线 | 第1轮 | 第2轮 | 总降幅 |
|---|---|---|---|---|---|
| TxnSet | allocs/op | 25 | 15 | 14 | -44% |
| TxnSetParallel | allocs/op | 18 | 9 | 8 | -56% |
| BatchSet10 | allocs/op | 84 | 74 | 64 | -24% |
| BigValue | allocs/op | 14 | — | 8 | -43% |
| WriteRequest | allocs/op | 15 | 15 | 15 | 0%(路径未涉及) |

> 注:ns/op 受 fsync 噪声主导(串行 ~4ms),Parallel 稳定在 2.0-2.3ms;allocs/op 是稳定指标。
