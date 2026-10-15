# TrainKV 读流程优化报告:从 2440ns 到 1800ns

> 一轮完整的读路径优化:1 个 25% 的性能浪费(带正确性论证)、2 个微优化、2 个负收益实验、
> 2 个测量失真基准的修复、1 个缓存并发优化,以及顺路修掉的 3 个隐藏问题。
> 量化结果全部来自本机实测(Intel i5-7300HQ / WSL2 / Go 1.25,`-4` 表示 GOMAXPROCS=4)。
> 配套改动:lsm/lsm.go、lsm/memTable.go、model/Key.go、utils/cache/cache.go、db.go、
> benchmk/bench_trainkv_test.go、skl/skipList_test.go(修测试)、vlog_test.go(新增回归测试)。

## 一、起点:基准与 profile 定位

`BenchmarkReadGetHitRandom`(100K key × 512B,随机点读):

```
基线:2367 ns/op,120 B/op,2 allocs/op
```

profile 两个关键发现:

1. **数据根本没落盘**:注释声称"100K×512B ≈ 50MB > 10MB memtable 触发轮转",但默认 `MemTableSize` 实际是 **64MB**——全部数据都在 memtable 跳表里,这测的是纯内存点读。
2. **热点分布**:`findNear`(跳表下潜)累计 48%,`getKey` 内联 flat 30%,`cmpbody` 7%,外加 **LevelsManger.Get + LevelHandler.Get + searchLnSST 合计约 25%**——后者是纯浪费(见下)。

## 二、核心问题:memtable 命中后白搜 8 层(收益最大的改动)

**现象**:命中后不直接返回,而是继续搜索 L0~L7 全部 8 层,每层 RWMutex 加解锁 + 二分,最后返回的还是同一个 entry。

**根因**(`lsm/lsm.go` 的 `LSM.Get`):早返回条件是 `entry.Version == startTs`,而事务时间戳分配是 `startTs = nextTxnTs - 1`,恰好等于**最近一次提交的 commitTs**——所以只有"最后一批提交"的数据能命中 `==`,更早的数据(基准里 99.5%)版本 < startTs,全部掉进全层搜索。

**方案**:命中且 `Version <= startTs` 时提前返回。

**量化**(本机):

| 基准 | 基线 | 方案一后 | 降幅 |
|---|---|---|---|
| ReadGetHitRandom | 2367 ns/op | **1729** ns/op | **-27%** |
| ReadGetHitSeq | 1224 ns/op | **801** ns/op | **-35%** |

## 三、正确性论证:这道坎过了两遍

### 第一遍:为什么 `<=` 与 `==` 等价(在无干扰下)

三条引理,逐条用代码核实:

1. **命中即可见**:跳表 key 编码为 `big-endian(MaxUint64−ts)`,`findNear(less=false, allowEqual=true)` 返回"第一个 ≥ 搜索 key 的节点"。因此命中节点必满足 `version ≤ startTs`——版本 > startTs 的节点编码值更小,排在搜索位置之前,永远不会被命中。这也正是"读不到未来数据"的机制本身。
2. **memtable 命中即全局最新**:跳表每个 raw key 只保留一个节点(写入统一走 `Put` 原地更新),且存储层新旧顺序 = 版本顺序(mt → imm → L0 → Ln,commitTs 严格递增)。
3. **全层搜索找不到更好的**:由 2,SST 里该 key 的版本必然 < memtable 命中版本,既不存在 `== startTs` 的精确匹配,也不可能超过已记录的最大版本。旧代码搜完 8 层返回的**就是同一个 entry**。

### 第二遍:用户发现的不变量破坏 → BitTxn 门控

论证依赖的不变量"存储层新旧顺序 = 版本顺序"被 **vlogGC 破坏**:`gcReWriteLog` 会把仍被 LSM 引用的旧版本**复活进最新 memtable**(条目无 BitTxn 标记),而更新的版本可能在 imm/L0 里。此时 `<=` 早返回会读到旧数据(只需 memtable 轮转,无需落盘)。

**最终方案**:早返回加 **BitTxn 门控**——仅普通事务条目(带 BitTxn)可提前返回;GC 重写条目(无 BitTxn)走全层取最大兜底。精确版本匹配(`== startTs`)不设门控,因为此时返回的必然是正确的新指针副本。

**验证**:全量测试通过;新增回归测试 `TestVlogGCRewriteInversion`(构造"旧版本被 GC 重写 + 轮转"场景)——去掉门控的 naive `<=` 下测试**失败**,证明测试确实能抓住这个 bug。

## 四、微优化(合计约 1%)

### 比较函数:省掉每次 4 次切片构造

`CompareKeyWithTs` 原来每次比较都做 `key1[:len-8]`、`key2[:len-8]` 两次切片。优化为**长度感知**:

- 长度相同:直接全量 `bytes.Compare`(首个不同字节要么在 raw key 区,要么在 ts 区,语义一致);
- 长度不同:先比公共 raw key 前缀,前缀短者小(与 ts 无关)。

**踩坑**:第一版我直接改成全量 `bytes.Compare`,并自认为证明了等价——**被 compaction 测试抓出漏洞**:raw key 存在前缀关系时(如 "foo" vs "fooz"),首个不同字节可能落在 ts 区,全量比较会翻转结果。修正为上面的长度感知版本后全量测试通过。教训:核心比较函数的"等价证明"要用前缀用例验证,不能只验证等长输入。

### 移除冗余引用计数

`MemoryTable.Get` 内部多做了一次 `skipList.IncrRef/DecrRef`,而 `LSM.Get` 外层已持有 memtable 引用(`MemoryTable.Get` 唯一调用方)。移除后每次读省 2 次原子操作。

**量化**:1798 → 1790 ns/op(噪声内,~0.5%)。

## 五、负收益实验:软件预取(记录,不作为方案)

跳表下潜是串行依赖链(必须比完当前节点才知道下一跳),尝试用 dummy load 软件预取掩盖 cache miss:

| 方案 | 结果 | 结论 |
|---|---|---|
| 预取"同层前进候选" | 1790 → **2040 ns/op**(-13%) | 下潜路径上预取目标大多不是实际下一跳,纯添乱 |
| 预取"降层候选" | 1790 → **1899 ns/op**(-6%) | 同样负收益 |

结论:Go 无显式预取指令,dummy load 的指令开销 + 低命中率,在本硬件上不划算。已全部还原。

## 六、残余成本:跳表下潜的内存延迟(结构性结论)

微优化全部做完后仍 ~1.8µs,profile 显示 60%+ 在 `findNear`。成本结构:

- 一次 Get ≈ 16-20 跳,每跳**串行**等 2 行冷缓存(节点字段 + key 字节,在 57MB arena 中相距 ~560B),约 80-100ns/跳;
- 固定开销 ~200-300ns(KeyWithTs 分配、锁、引用计数、entry 构造);
- 依赖链是根本:无法并行,预取实验失败也印证了这点。

**结论**:这是 skiplist memtable 随机点读的物理成本(Badger 同级数据同量级),不是代码缺陷。真正的结构性提速需要换索引结构(见"遗留方向")。

## 七、测量修复:两个失真的 benchmark

`bench_trainkv_test.go` 的 `Benchmark_TrainKV_PutValue/GetValue` 有三个硬伤:

1. **init() 清空目录 A、打开目录 B**(`perfDataDir`),DB 状态依赖历史运行,不可复现;
2. **GetValue 的 setup 在每次 benchmark 调用(含校准迭代)里重写 500K 条**,计时区外写入的后台 flush/compaction 分配仍会计入 `B/op`;
3. **读范围超出写入范围**(`i ∈ [0, b.N)` 只写了 0..499999),约 1/3 读是 miss。

修复:每个 benchmark 独立清目录打开、setup 移到 `ResetTimer` 前并等待落盘稳定、读用预生成随机序且取模限制在写入范围内。

**量化**(同一台机器):

| 基准 | 修复前 | 修复后 | 说明 |
|---|---|---|---|
| TrainKV_GetValue | 13989 ns/op,4383 B/op,**30 allocs** | **4590 ns/op,825 B/op,7 allocs** | 30 allocs 是 setup + 后台合并的污染 |
| TrainKV_PutValue | ~11035 ns/op | 12678 ns/op | 写路径数字本来就大体可信,目录修复后量级不变 |

注意:GetValue 的降幅是**测量修复**而非读路径代码优化;修复后它测的是真实负载(200K 条全落盘 L0 的随机点读)。

## 八、SST 点读路径:一次误诊与一个并发优化

修复后的 GetValue profile(SST 路径,~4600 ns/op)第一大热点是 `runtime.futex` 12%,最初判断为**块缓存全局写锁竞争**(W-TinyLFU 每次访问都拿写锁做 LRU 晋升/准入统计)。

**实施块缓存 16 分片**(`utils/cache/cache.go`):`Cache` 改为分片路由,完整 W-TinyLFU 下沉到 `cacheShard`,乘法散列路由避免结构化低位倾斜,公共 API 不变。

**但 pprof -peek 修正了诊断**:futex 的 97% 来自 `runtime.notesleep/notewakeup`(GC 工作协程协调),mutex 竞争(`semasleep`)仅 2.7%——单线程读路径根本没有锁竞争,12% 其实是**分配 churn 带来的 GC 开销**(825 B/op × 200 万次操作 ≈ 1.6GB 分配)。

分片本身仍有真实价值(多协程场景):

| 基准 | 分片前 | 分片后 | 降幅 |
|---|---|---|---|
| CachePerformance(4 协程并行 Set+Get) | 685 ns/op | **361 ns/op** | **-47%** |
| TrainKV_GetValue(单线程) | 4590 | 4587 | 中性(无回归) |

## 九、顺路修掉的隐藏问题

| 问题 | 根因 | 修复 |
|---|---|---|
| `db.get` 空回调 panic | 读协程持旧 ValuePtr 与 vlogGC 删除文件竞态,`vlog.Read` 出错返回 nil 回调,`defer callBack()` 解引用崩溃 | 回调非空才 defer(非空 = 文件读锁已持有,无论成败必须释放) |
| `TestSkipListUpdate` 随机失败(~3%) | 测试用无 ts 的随机大写 key 查询,`CompareKeyWithTs` 对短 key 只比较前 `len-8` 字节,前缀 2 字节碰撞即误命中 | 查询改用不可能碰撞的 key,60 连跑通过 |
| CompareKeyWithTs 全量比较漏洞 | 前缀 key("foo"/"fooz")的首个不同字节可能落在 ts 区,全量比较语义错误 | 长度感知版本(见第四节),被 compaction 测试抓住 |

## 十、量化汇总

| 项 | 基线 | 最终 | 降幅 |
|---|---|---|---|
| 随机点读(memtable 命中) | 2367 ns/op | ~1800 ns/op | **-24%** |
| 顺序点读(memtable 命中) | 1224 ns/op | 801 ns/op | **-35%** |
| 并行块缓存吞吐 | 685 ns/op | 361 ns/op | **-47%** |
| GetValue 基准(测量修复) | 13989 ns/op / 30 allocs | 4590 ns/op / 7 allocs | 测量口径修正 |
| 分配 | 120 B/op,2 allocs/op | 不变(读路径零新增分配) | — |

回归保障:全量 `go test ./...` 零失败;新增 `TestVlogGCRewriteInversion`(GC 场景读正确性)、reopen 持久化测试;`-race` 通过。

## 十一、遗留方向(未实施)

1. **memtable 点读哈希索引**:`map[rawKeyHash]nodeOffset` 直查,一次随机访问取代 16-20 跳,预期 3-4x;代价是写入路径同步维护、并发正确性、恢复/刷盘一致性、~5-10MB 内存,属结构性改动。
2. **SST 读惰性值(零拷贝)**:命中时 512B 值的 `SafeCopy` 是单线程 SST 读的最大单项分配(连带 GC 开销),需引入类似 Badger 的 `Value()`/`ValueCopy()` 双接口,属 API 语义改动。
3. **CacheNums 与数据集匹配**:默认 10K 格 × 4KB = 40MB,装不下大数据集,块缓存 miss 直接读盘。
4. **vlogGC 与点读的竞态**:点读不持有 vlog 文件引用,GC 删除文件时正在读的协程会拿到瞬时"文件不存在"错误(已从 panic 降级为 error),彻底解决需 GC 侧引用计数或读重试。

## 十二、经验清单

1. **先证明测量对象,再优化代码**:基准数据是否真的在 memtable、读范围是否越界、后台协程是否污染计时——先修测量,再谈优化。
2. **核心比较函数的等价性证明要用前缀用例验证**:等长输入通过不代表前缀关系下语义一致,测试套件能抓住这类错误。
3. **pprof 的 flat 大块不一定是指令开销**:`getKey` 内联 40% flat 是缓存行等待(stall 记在消费指令上);判断锁竞争要用 `pprof -peek` 看调用栈(futexsleep 来自 notesleep = GC,来自 semasleep = 互斥锁)。
4. **软件预取在 Go 里要实测,别凭直觉**:两个方向都负收益,指令开销盖过了预取收益。
5. **正确性论证要列出依赖的不变量**:`<=` 早返回依赖"存储层新旧顺序 = 版本顺序",而 vlogGC 会破坏它——最终用 BitTxn 标记把"可能反转的条目"全部排除在快路径之外,竞态不是被缩小而是被消除。
6. **同一份代码,单线程与多线程的瓶颈不同**:缓存分片对单线程中性、对 4 协程并行 -47%,优化前先想清楚目标负载。
