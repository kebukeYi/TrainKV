# TrainKV 面试 100 题:从基础到系统设计

> 配套 `docs/resume.md` 项目介绍。本文与 `docs/interview-qa.md`(30 题)、`docs/interview-prep.md`(17 题)
> 的关系:**这两份偏"简历 bullet 定点问答",本文是覆盖面更广、难度递进的 100 题**,额外补齐了
> WAL 格式细节、MANIFEST 原子写、压缩调度与冲突互斥、AtomicMark 死锁、分片路由、基准测量失真、
> 跨平台 mmap、系统设计延伸题等。
>
> 组织方式:五层难度递进。每题给 **考察点 → 参考答案 → 追问**。答案全部落到本项目真实实现,
> 涉及的函数名、常量、行号级机制、性能数字都可在代码或 `docs/` 的基准报告里核对。
> 面试前建议:**第一、二层脱稿复述,第四层记住"故事 + 数字",第五层准备自己的观点而非标准答案**。

---

## 目录

| 层 | 主题 | 题号 | 面试期望 |
|---|---|---|---|
| 一 | 基础概念与动机 | Q1–Q12 | 讲得清全貌,不被术语唬住 |
| 二 | 核心机制(存储/WAL/压缩/格式) | Q13–Q32 | 每个设计选择能说出"为什么" |
| 三 | 事务、MVCC 与并发 | Q33–Q52 | 能画出时间线,讲清竞态与水位 |
| 四 | 工程落地:bug、性能、测试 | Q53–Q76 | 有故事、有数字、有方法论 |
| 五 | 系统设计与复盘(开放题) | Q77–Q100 | 有取舍判断和演进思路 |

---

# 第一层:基础概念与动机(Q1–Q12)

### Q1. 用一分钟介绍这个项目

**考察点**:能不能把一万行代码压缩成一句能听懂的话。

**参考答案**:
TrainKV 是我参照 Badger 的思路、用纯 Go 从零实现的 LSM-Tree KV 存储引擎,核心约一万行、无 CGO。写入走内存跳表(memtable)+ WAL,刷盘成 L0–L6 分层 SSTable,由后台压缩归并;超过 1MB 的大 value 走 kv 分离,追加进 value log,LSM 里只留 12 字节指针。上层是 TSO 时间戳的 MVCC 事务,支持乐观并发下的读写冲突检测;读路径有 W-TinyLFU 块缓存和 SST 布隆剪枝;崩溃恢复靠 WAL 重放加 MANIFEST 表登记。

我做的工作分两类:**一是把生产级关注点做完整**——事务、分层压缩的三条路径、vlog 垃圾回收、崩溃恢复;二是**用测试和基准逼出深层缺陷并做量化优化**——修了 15 个以上正确性问题(最有代表性的是块缓存晋升交换导致 1.4% 的 key 静默丢失),写路径分配从 25 降到 14 allocs/op,扫描做到 SST 路径零分配,大 value 惰性读取让扫 500 条 1MB 的数据搬运量从 528MB 降到 65KB。

**追问**:这一万行里最难的 100 行是哪里?→ 心理准备题,答题要点是 `utils/atomicMark.go`/`limiterMark.go` 的水位推进(死锁)或 `lsm/compact.go` 的版本回收判据。

---

### Q2. 为什么不用现成的 Badger/RocksDB,要自己写一个?

**考察点**:动机是否真实,是否清楚自研边界。

**参考答案**:
三个理由。第一,**只有从零写才能把设计决策理解到能做取舍的程度**——为什么压缩要分层、为什么 kv 分离、为什么 vlog GC 要靠压缩统计,读源码时是"知道",写的时候才变成"理解"。第二,**TrainKV 是能整库通读的完整实现**:Badger 约 3 万行、RocksDB 30 万行,我压到 1 万行,事务、GC、崩溃恢复、端到端测试都在,不是玩具。第三,纯 Go 无 CGO,单二进制部署。

同时我清楚定位:不与 RocksDB 正面竞争。`docs/comparison.md` 里我主动列了短板——无数据压缩、无列族、无 Range Delete/Merge、压缩调度是 50s 定时器、零生产验证。

**追问**:哪里借鉴了 Badger?差异在哪?→ 结构上借鉴了 kv 分离、MANIFEST、group commit、W-TinyLFU;差异是自己的迭代器分块 arena 零分配扫描、大 value 惰性读取、把三条压缩路径的死代码修活。**"修活死代码"这件事 Badger 靠长年生产迭代发现,我靠测试逼出来,这是这个项目的价值主张**。

---

### Q3. LSM 为什么写快读慢?三种放大分别是什么?

**考察点**:LSM 的立身之本。

**参考答案**:
- **写快**:写只做顺序追加——进内存跳表 + 追加 WAL,没有随机写、没有原地更新;配合 group commit 摊销 fsync,延迟不随数据量增长。
- **读慢**:读要逐层查——memtable → immutable memtable → L0 全部表 → L1..L6 每层二分。

三种放大:
- **写放大**:一个 key 反复写,WAL 一次、flush 一次、每层压缩再写一次。本项目 L0–L6 共 7 层。
- **读放大**:一次 Get 的磁盘访问次数 = L0 表数 + 每层至多 1 次块读;布隆过滤器把不存在的 key 短路。
- **空间放大**:被覆盖的旧版本和删除标记直到压缩才清理。

一句话本质:**压缩就是拿写放大换读放大和空间放大**,三者不可同时最优。

**追问**:你的实现里读放大具体是多少?→ L0 最多 `NumLevelZeroTables=5` 张表全查,加上 memtable 和一个 immutable,再 L1–L6 各一次二分;布隆命中率低时近似 5+6 次潜在 IO。这个数字直接决定 `NumLevelZeroTables` 该设多大。

---

### Q4. memtable 为什么用跳表而不是 B+ 树?

**考察点**:数据结构选型。

**参考答案**:
三条。① 跳表 O(log n) 查找插入、**无需再平衡**;② 节点间是原子指针,**读天然无锁**——本项目写路径被写通道串行化(单写者),但读来自任意协程,跳表恰好适配"单写多读";③ 节点分配走 arena,一次大块分配、偏移量寻址,避免逐节点 malloc 的碎片与 GC 压力。B+ 树要做到并发插入需要复杂的 latch coupling,实现成本高。

**追问**:那为什么不用 Redis 那种平衡树?为什么 maxHeight=20?→ 底数问题:跳表每层晋升概率 `heightIncrease = MaxUint32/3 ≈ 1/3`(见 `skl/skipList.go` 的 `randomHeight`),P(height≥k)≈(1/3)^(k-1);20 层对 2^32 量级节点都够。

---

### Q5. 从 `txn.Set` 到数据真正落盘,完整链路走一遍

**考察点**:写路径全貌。

**参考答案**:
1. `txn.Set(k,v)` → `model.NewEntry`(从 EntryPool 借)→ `modify`:登记写集 `pendingKeys[MemHash(key)]`,若开启冲突检测再登记 `conflictKeys` 的两个哈希,`entry.Version = startTs`。
2. `txn.Commit()` → `commitAndSendToDB`:在 `writeChLock` 内定 `commitTs`、做冲突检测、把 entry 的 key 换成 `KeyWithTs(key, commitTs)` 并打 `BitTxn`、按 key 排序、追加一条 `BitFinTxn` 结束标记,然后 `SendToWriteCh`。
3. `handleWriteCh` 单协程攒批(阈值 900,容量 1000),攒够或通道空则组一个 `WriteRequest`。
4. `WriteRequest`:先 `vlog.Write(reqs)`(大 value 落 vlog),`SyncWrites` 时 `vlog.Sync()`;再逐条 `writeToLSM`——大 value 换成 12 字节 `ValuePtr` 并置 `BitValuePointer`,`Lsm.RotateIfNeed` 预判轮转,`Lsm.Put` 追加 WAL 并插入跳表;最后 `SyncWalFile()`。
5. 请求的 `Wait()` 被唤醒,`doneCommit(commitTs)` 推进 `commitMark`,提交者拿到 commitTs 返回。

**关键顺序**:**值先于引用落盘**——vlog sync 在 WAL sync 之前,否则 WAL 里的指针可能指向未持久化的 value。

**追问**:哪一步是瓶颈?→ 默认 `SyncWrites=false` 时全程无 fsync,单条写约 3.2µs;开启后每批一次 fsync,单条约 4ms,靠 group commit 摊到并发提交上。

---

### Q6. 一次 `Get` 的完整链路走一遍

**考察点**:读路径全貌 + MVCC 可见性落在哪。

**参考答案**:
1. 若在事务里:先查 `pendingKeys` 实现**读己之写**;未命中则 `addReadKey` 登记读集,再 `ensureStartTsReady()`(写事务首次读 LSM 时才等 `commitMark`)。
2. `KeyWithTs(key, startTs)` → `LSM.Get`。
3. `LSM.Get`:持 `sync.RWMutex` 读锁快照 memtable + immemoryTables(倒序,新→旧),逐个 `IncrRef`;命中 memtable 且**带 `BitTxn` 且 `Version <= startTs`** 则提前返回;否则下探 `LevelsManger.Get`。
4. L0 线性扫全部表(新表优先),Ln 每层二分定位唯一可能命中的表;每张表先过布隆,再二分索引定位 block,先查 W-TinyLFU 块缓存,未命中才读盘。
5. 大 value 条目 `Meta & BitValuePointer` → 解析 `ValuePtr` → `vlog.Read` 拿 mmap 零拷贝切片 → `SafeCopy` 出锁外使用 → 归还 callBack(释放文件读锁)。
6. 事务层 `IsVisible`: `startTs >= ParseTsVersion(entry.Key)`;`IsDeletedOrExpired` 判墓碑/过期返回 `ErrKeyNotFound`。

**追问**:可见性到底在哪一层判?→ 存储层只做"返回版本 ≤ startTs 的最新一条"(`BitTxn` 门控的早返回),事务迭代器再做一次 `IsVisible` 过滤;**物理删除旧版本只在压缩时按水位做**。

---

### Q7. SST 的文件布局是什么?怎么定位一个 key?

**考察点**:磁盘数据结构基本功。

**参考答案**:
SST 由 `sstBuilder` 顺序构建,布局是:

```
[block0][block1]...[blockN-1] | index(pb.TableIndex) | idxLen(4B) | checksum(8B) | checksumLen(4B)
```

- 每个 data block 约 `BlockSize=4KB`,条目做**前缀压缩**(与 block 首 key 的公共前缀,`entryHeader{overlap,dif}`),块尾是 restart points(`entryOffsets []uint32`)+ 条目数 + 校验和。
- `TableIndex` 含每个 block 的 baseKey/offset/size、布隆过滤器、`KeyCount`、`MaxVersion`、`StaleDataSize`。
- 打开时从尾部倒读:4B 取 checksumLen → checksum → 4B 取 idxLen → index,校验后反序列化,索引常驻内存。

查 key:布隆剪枝 → 在 `Offsets` 上二分找第一个 `baseKey > keyTs` 得 `blockIdx=lo-1` → `getBlock`(先查块缓存)→ 块内对 `entryOffsets` 二分 → 命中则 `SafeCopy` value。

**追问**:块边界把同一个 key 的多个版本切开了怎么办?→ `blockIterator.Seek` 返回 `ErrBlockEOF` 时回退到 `blockIdx+1` 再查一块(见 `lsm/table.go` 的 `searchBlock` 与 `TableIterator.seekFrom`)。

---

### Q8. key 为什么要把时间戳编码进尾部,而且取反?

**考察点**:MVCC 在存储层的落地方式。

**参考答案**:
编码是 `KeyWithTs(key, ts) = key ‖ BigEndian(MaxUint64 - ts)`(见 `model/Key.go`)。取反的原因是**让排序语义和扫描语义对齐**:
- 存储层按字节序升序排列后,同一个 raw key 的**版本号大的排在前面**(新版本在前);
- 于是 `findNear` 找"第一个 ≥ 搜索 key"返回的就是 ≤ 请求版本的最新一条,一次下潜搞定 MVCC 点查;
- 归并迭代器天然按 raw key 升序、同 key 版本降序输出,去重只需"同 raw key 只取第一条"。

`ParseTsVersion = MaxUint64 - BigEndian(key[-8:])`,`ParseKey` 去尾 8 字节,`SameKeyNoTs` 比较 raw key。

**追问**:比较函数有什么坑?→ `CompareKeyWithTs` 必须是**长度感知**的:长度相同直接 `bytes.Compare`,长度不同先比公共 raw key 前缀。我第一版图省事改成全量 `bytes.Compare`,**被压缩测试抓出 bug**——raw key 存在前缀关系时(如 "foo" vs "fooz"),首个不同字节可能落在 ts 区,结果整个翻转。教训:核心比较函数的等价证明要用前缀用例验证。

---

### Q9. L0 为什么允许 key 重叠,L1+ 为什么不重叠?

**考察点**:分层设计的核心权衡。

**参考答案**:
相邻 memtable 的 key 范围天然重叠(不同时刻写同一批 key),flush 时**直接把 memtable 倾倒成 L0 表、不做归并**,所以 L0 允许重叠。这是**拿读放大换写路径简单**——写路径不用等归并,延迟低。

代价是 L0 读要查全部表(逆序、新表优先、命中即停),所以必须控制 L0 表数:`NumLevelZeroTables=5`。L1 以上每层表范围互不重叠,读只需每层一次二分定位。L0→Ldst 压缩把多张重叠表归并成有序的下一层表,同时清理被覆盖的旧版本。

**追问**:L0 表如何排序?→ 按 `MaxVersion` 降序(新数据在前),见 `levelHandler.addAndSize` 与 `Sort`;非 L0 按 `MinKey` 升序。

---

### Q10. 布隆过滤器怎么实现?参数怎么定?

**考察点**:概率数据结构的实际参数。

**参考答案**:
见 `utils/bloom.go`,leveldb 同款:
- 位数组 + 最后 1 个字节存哈希个数 `k`;`MayContain(h)`:若 `k > 30` 视为"预留编码"直接返回 true,否则用 double hashing `delta = h>>17 | h<<15`,循环 k 次 `bitPos = h % nBits` 探测,任一位为 0 即不存在。
- 参数:`BloomBitsPerKey(n, fp) = ceil(-n*ln(fp)/ln(2)^2 / n)`,默认 `BloomFalsePositive=0.01` → `-ln(0.01)/ln(2)^2 ≈ 9.6` → **约 10 bits/key**;`k = uint32(bitsPerKey*0.69)` 夹到 [1,30]。
- 构建时 key 用**去掉 ts 的 raw key**(`sstBuilder.go`),所以同 key 多版本共享一个过滤器,查表时 `MayContainKey(ParseKey(keyTs))`。

**追问**:0.01 的误判率换成内存是多少?→ 10 bits/key,1000 万 key 约 12MB;如果改成 0.001 会到约 14.4 bits/key。过滤器随 `TableIndex` 常驻内存,是小内存换 IO 的典型交易。

---

### Q11. 块缓存为什么需要?和 OS page cache 是什么关系?

**考察点**:是否理解"重复造轮子"的合理性。

**参考答案**:
OS page cache 缓存的是**文件页**,而我们读 SST 是 mmap 后按 block 解析——即使页在 page cache 里,每次仍要做块尾倒读、checksum 校验、`entryOffsets` 二分、前缀解压这些 **CPU 工作**。块缓存缓存的是**已解析好的 `*block` 结构**,命中后直接做块内二分、零解析零 syscall。

所以两者是互补的层级:page cache 省磁盘 IO,块缓存省解析 CPU。本项目 `CacheNums=10240`,大约对应 40MB 块数据。

**追问**:缓存什么时候失效?→ 引用计数归零、SST 文件被删时 `Table.DecrRef` 逐块 `Del`(否则会缓存到已删除文件的块)。压缩和恢复路径用 `IsSetCache=false`,不污染缓存。

---

### Q12. 什么是 MVCC?KV 存储为什么需要它?

**考察点**:概念落地的能力。

**参考答案**:
MVCC(多版本并发控制)让读不阻塞写、写不阻塞读:每次写产生新版本而不是覆盖旧版本,读事务按自己的快照时间戳看到对应版本。落到 TrainKV:
- 每次提交分配全局单调的 `commitTs`,写 entry 的 key 编码里带上它;
- 读事务开启时定 `startTs`,只读 `version ≤ startTs` 的版本;
- 旧版本不会立刻删,等**所有 startTs ≤ T 的读者都结束**(水位推进到 T)才在压缩时回收。

需要的三个理由:① LSM 本身只追加不改写,天然多版本,顺水推舟;② 快照读保证长事务不会被并发写干扰,这是事务语义的基础;③ 顺序扫描时能拿到一致的视图。

**追问**:代价是什么?→ 空间放大(旧版本要留)和读放大(同 key 多版本要跳过),以及**水位管理本身的复杂度**——本项目最难的并发代码就在水位(`AtomicMark`/`LimitMark`)。

---

# 第二层:核心机制(Q13–Q32)

### Q13. WAL 的记录格式是什么?

**考察点**:是否真的自己动手写过格式。

**参考答案**:
见 `lsm/wal.go` 的 `EncodeAt`:

```
| header(变长) | key | value | crc32(4B) |
header = Meta(1B) | uvarint(KLen) | uvarint(VLen) | uvarint(ExpiresAt)
```

- `WalHeaderSize=21` 是变长编码的上界(`common.MaxHeaderSize=21`)。
- CRC 用 **Castagnoli 多项式**,覆盖 header+key+value,追加在末尾。
- 文件按 `MemTableSize` 预分配 mmap,越界时 `Truncate` 一次扩到 `MaxSz`。
- **没有 record type,没有 chunk 分片**:每条 entry 一条独立记录,靠 `header.KLen == 0` 判定文件尾(零填充区)。这是与 RocksDB/WAL 分片模型的差异点。

**追问**:为什么不用分片?→ 简化实现,且本项目 WAL 单条即一个完整 entry(不像 RocksDB 要支持跨块大 value 流),不过代价是"撕裂写"只能靠 CRC + 全零判断来恢复。

---

### Q14. 一条 WAL 记录写了一半就崩溃,恢复时怎么处理?

**考察点**:崩溃恢复的细节功夫。

**参考答案**:
见 `WalDecode` 与 `WAL.Read`:
- `header.KLen == 0` → `io.EOF`,视为正常结尾(预分配区的零填充);
- 读 key+value 时 `io.EOF` → `ErrTruncate`;校验和读不齐 → `ErrUnexpectedEOF`,都当正常结束返回 `(nil, 0)`;
- CRC 不匹配 → `ErrWalInvalidCrc`,再往后看是否**全零**:全零说明是崩溃撕裂的尾部,容忍;否则说明是文件中部损坏,**直接 panic**(真损坏不能装作没事)。

恢复完拿到 `validEndOffset`,`openMemTable` 用它对 WAL 截断到最后一个完整记录。

**追问**:我的崩溃测试怎么造这个场景?→ `crash_test.go` 的 `corruptWalTail`:读 `.wal`,找到"前 64 字节全零"的零填充区起点作为有效数据边界,用 12 个 `0xDE` 覆盖 `buf[dataEnd:dataEnd+12]`,精确制造半写撕裂记录,再重开校验。

---

### Q15. 一个多 key 事务的原子性怎么保证?WAL 里怎么表达事务边界?

**考察点**:WAL 事务原子性,常被问。

**参考答案**:
用两个 Meta 位标记事务边界:
- 事务内每条 entry 打 `BitTxn`,key 里带 `commitTs`;
- 事务末尾追加一条 **finTxn**:`key = common.TxnKey("!train!txn!fin")`,`value = commitTs 十进制明文`,`Meta = BitFinTxn`。

重放时 `recovery2SkipList` 的语义:
- 遇 `BitTxn`:记录首条的 `lastCommitTs`,后续必须相等,否则 `ErrBadTxn`;数据先**缓存在内存里不落跳表**;
- 遇 `BitFinTxn`:`ParseUint(value)` 必须等于 `lastCommitTs`,否则 `ErrBadTxnFin`;匹配后把这批 entry 一次性 `put` 进跳表,推进 `validEndOffset`;
- 文件结束时若还有未配对的 `BitTxn`,这些数据**从不进跳表**,`validEndOffset` 停在最后一条完整记录——这就是"未提交事务被丢弃"。

**追问**:为什么 finTxn 的 value 存明文 commitTs 而不是二进制?→ 出于可读性和简单性,但正是这个"明文"设计暴露了一个 bug(见 Q30)。

---

### Q16. 崩溃后残留一个"孤儿 FIN"或"空事务",会怎样?

**考察点**:边界场景,体现测试深度。

**参考答案**:
三处加固:
1. **孤儿 FIN 跳过**:重放遇到 `BitFinTxn` 时若 `lastCommitTs == 0`(说明它前面没有对应的 `BitTxn`,是崩溃残留),直接跳过而非报错(`memTable.go` 的 `recovery2SkipList`)。
2. **空事务不落 WAL**:`commitAndSendToDB` 里如果 `pendingKeys` 为空,直接返回 `doneCommit` 闭包,**不写 WAL**。否则会产生一条纯 finTxn 记录,重开时就是孤儿 FIN → `ErrBadTxn`。修复前 `TestDBEmptyTxnCommitReopen` 是失败的。
3. **请求级轮转预判**:`LSM.RotateIfNeed(entries)` 让同一请求的**数据 + finTxn 落在同一个 WAL/memtable**。否则 finTxn 落进新 WAL、数据留在旧 WAL,旧 WAL flush 后被删,重开就是无主 FIN。

**追问**:这三处是同一类问题吗?→ 是:**finTxn 和数据必须在同一个原子单元里**,否则要么丢数据要么报错。三处是三个不同的破坏路径,分别来自"调用方不写数据""崩溃""轮转"。

---

### Q17. MANIFEST 的格式和作用是什么?

**考察点**:元数据管理。

**参考答案**:
MANIFEST 记录"当前有哪些 SST 表在哪些层"(`Manifest{Tables, Levels}`)。格式(见 `lsm/manifest.go`):
- 头部 8B:`MagicText "MAGC"` + `MagicVersion`,校验不匹配直接拒绝;
- 之后是重复记录:`| dataLen(4B) | crc32(4B) | dataBuf |`,`dataBuf` 是 `pb.ManifestChangeSet`(Create/Delete 表的变更集)。

作用:
- 决定**哪些文件是活的**;重启时 `checkSSTable` 删除目录里未被引用、或引用了却缺失的表;
- SST 生命周期:`LevelsManger.maxFID` 分配 tableId,压缩产生的新表先 `AddTableMeta` 记进 MANIFEST,再挂到层上。

**追问**:MANIFEST 会不会无限增长?→ 会。`AddTableMeta` 里当 `Deletions > 10000` 且 `Deletions > 10*(Creations-Deletions)` 时触发 rewrite(见 `ManifestDeletionsRewriteThreshold` / `ManifestDeletionsRatio`)。

---

### Q18. MANIFEST 是怎么做到崩溃安全的?为什么必须是"先写 MANIFEST 再挂表"?

**考察点**:原子写与崩溃窗口。

**参考答案**:
两条:
1. **追加记录的原子性**:每条变更带 CRC,重放时 `io.EOF/ErrUnexpectedEOF` 视为正常结束,CRC 不匹配视为尾部截断,`OpenManifestFile` 用返回的 `truncOffset` 把半写记录截掉。所以单条变更要么完整生效要么完全忽略。
2. **rewrite 用 rename 原子替换**:`doRewrite` 写 `REWRITEMANIFEST` 临时文件 → `Sync` → `Close` → `os.Rename` 覆盖 `MANIFEST` → `SyncDir`。rename 在同目录下是原子的,不会出现"半个 MANIFEST"。

顺序上必须是**先把变更落进 MANIFEST,再把表挂到内存层结构**。反过来(先挂表后写 MANIFEST)如果中途崩溃,重启后 MANIFEST 里没有这张表 → 表文件变成孤儿被 `checkSSTable` 删除,但数据可能只存在于这张表 → **数据丢失**。当前顺序崩溃最多是"MANIFEST 里登记了但目录里没有文件",重启时能检测 `ErrTableFileMissing` 并报错,是可检测的不一致而非静默丢数据。

**追问**:压缩产出的新表什么时候真正可见?→ `runCompactDef` 先 `AddChanges` 到 MANIFEST,再更新 nextLevel/thisLevel 的表集,`defer decrTables()` 最后释放旧表引用。旧表在引用计数归零后才删。

---

### Q19. "崩溃后残留空 WAL 的库无法重开"——这个 bug 的根因是什么?

**考察点**:系统调用层面的一个真实坑(commit 4675bbc)。

**参考答案**:
根因是 `openMemTable` 无条件 `walFile.file.Truncate(int64(endOff))`。崩溃后可能残留一个**从未写过有效记录的预分配 WAL**(创建时文件被 `ftruncate` 到 `MemTableSize`,内容全零),此时 `endOff == 0`,`ftruncate(0)` 把文件长度变成 0,紧接着 mmap 的 `mremap(new_size=0)` 返回 **EINVAL** → **这类库永久无法重开**。

修复:只在 `endOff > 0` 时截断——保留稀疏的预分配文件即可,下次重放同样判定为空:

```go
// endOff == 0 表示 WAL 中无任何有效记录(崩溃后残留的"预分配全零 WAL");
// 此时不能再走 Truncate: ftruncate(0) 之后 mremap(new_size=0) 返回 EINVAL,
// 会让这类库永久无法重开;
if endOff > 0 { err = walFile.file.Truncate(int64(endOff)) }
```

**追问**:怎么发现的?→ 崩溃恢复测试里子进程写完就 `os.Exit(0)`,重开时命中;`TestOpenWithEmptyWAL` 是回归用例。教训:**mmap 场景下 `Truncate(0)` 是一个陷阱**,任何"清空文件"的操作都要小心它之后的 remap。

---

### Q20. memtable 的内存布局?为什么节点里存偏移量而不是指针?

**考察点**:内存布局与 GC 友好性。

**参考答案**:
`skipNode` 布局(`skl/skipList.go`):

```go
type skipNode struct {
    value     uint64            // 高32位=valSize, 低32位=arena 内 val offset
    keyOffset uint32
    keySize   uint32
    height    uint16
    next      [maxHeight]uint32 // 各层后继的 arena 偏移, 0 表示 nil
}
```

- 节点里 **key、value、后继指针全部存 arena 内的 uint32 偏移**。32 位偏移把 8 字节指针砍成 4 字节,`next[20]` 从 160B 降到 80B;而且**整个 arena 只有一个 Go 指针**,GC 扫描 memtable 时不需要遍历几百万个节点对象——这是 GC 友好的关键。
- value 字段用 `encodeVal(offset, size)` 打包成一个 uint64,**因为要原子读写整块**:`setVal` 用 `atomic.StoreUint64`,保证 value 更新对读者是原子的(不会看到 size 变了 offset 没变的中间态)。

**追问**:arena 满了怎么办?→ arena 是**固定容量、不可扩容**的,`allocate` 里 `AssertTrue(offset <= len(data))`,越界直接 fatal。所以 memtable 轮转必须**提前预判**:`MemBytes() + EstimateEntryMemSize(entry) > MemTableSize` 就轮转。`EstimateEntryMemSize = MaxSkipNodeSize + nodeAlign + len(key) + len(value) + 11`,是**上界**估算,保证写后绝不越界。

---

### Q21. 跳表的并发是怎么做的?为什么可以无锁读?

**考察点**:并发数据结构。

**参考答案**:
- **读完全无锁**:`findNear` 从 head 最高层下潜,只做 `atomic.LoadUint32` 读 `next[]`、`atomic.LoadUint64` 读 value。能这么做的前提是"节点一旦链入就不会被修改或删除"——跳表不删节点,更新是同 key 原地写 value(原子),新版本是**新 key(带新 ts)→ 新节点**。
- **写用 CAS**:插入时逐层 `casNextOffset(i, next[i], newNodeOffset)`,失败就重找 splice 重试。因为本项目写路径被写通道串行化,实际竞争很小,CAS 主要防的是与并发读的内存序问题。
- **同 key+ts 精确命中时原地更新**:`prev == next` 说明已存在完全相同的 keyTs(典型场景是 vlog GC 重写),直接 `PutVal + setVal`,不新增节点。
- 高度随机化和 `skipList.height` 的抬升也用 CAS。

**追问**:那 `num` 计数呢?→ `atomic.AddInt64`,因为读者/统计可能并发访问。生命周期用 `ref atomic.Int32` + `OnClose` 钩子,引用归零触发清理。

---

### Q22. memtable 什么时候轮转?为什么还需要一个"请求级"的 `RotateIfNeed`?

**考察点**:是否理解死锁式边界。

**参考答案**:
两个层次:
1. **逐条触发**(`LSM.Put`):`MemBytes() + EstimateEntryMemSize(entry) > MemTableSize` 就 `Rotate()`。这是兜底。
2. **请求级预判**(`LSM.RotateIfNeed(entries)`):累加整个请求(一个事务的全部 entry + finTxn)的估算,若整体放不下就**在写这批数据之前**先轮转。原因是 Q16 讲过的:**数据 + finTxn 必须落在同一个 WAL**。如果靠逐条触发,可能数据写进旧 WAL、finTxn 触发轮转落进新 WAL,旧 WAL flush 后被删 → 重开出现无主 FIN → `ErrBadTxn`。

若单个请求本身就超过 `MemTableSize`,则退回逐条行为(反正装不下,只能靠 WAL 顺序恢复)。

**追问**:轮转时怎么处理旧表?→ `LSM.Rotate` 加写锁,把当前 `memoryTable` 追加进 `immemoryTables`,新建空表;`SyncWrites` 下对旧表 WAL `SyncFile()`;然后把旧表推给 `flushMemTable` channel(容量 `WaitFlushMemTables=6`,形成**背压**)。

---

### Q23. memtable 刷到 L0 的流程?为什么 flush 队列有背压?

**考察点**:后台任务与背压。

**参考答案**:
- `StartFlushMemTable` 是**单 goroutine**:从 `flushMemTable` channel 取表,空表跳过,否则 `LevelManger.flush`——用跳表迭代器顺序遍历、`builder.Add` 构建 SST(`OpenTable`)、`manifestFile.AddTableMeta(0, ...)` 记到 L0、`levelHandlers[0].addAndSize(t)`。
- 完成后在 `LSM.Lock` 下 `immemoryTables = immemoryTables[1:]` 并 `im.skipList.DecrRef()`。**这里依赖 channel FIFO + 只从头部摘除**。
- 背压:`flushMemTable` 容量 `WaitFlushMemTables=6`,满了 `Rotate` 会阻塞。目的是防止产生无限多的 immutable memtable——每个 immutable 都占着内存且**读路径要逐个查**,不背压会导致内存和应用延迟都失控。

**追问**:为什么 flush 用单 goroutine 而不是并发?→ 简化 memtable 顺序摘除逻辑,且 flush 是顺序 IO,瓶颈通常在磁盘不在 CPU;并发 flush 还会让 L0 表更多、读放大更大。

---

### Q24. L0–L6 的分层容量是怎么定的?

**考察点**:`levelTargets` 的算法。

**参考答案**:
见 `compact.go` 的 `levelTargets()`,从末层往上推:
- 从 last level 开始 `levelTargetSize = totalSize`,谁先缩到 `BaseLevelSize` 以下谁当 `dstLevelId`(压缩的目标层);每往上一层 `totalSize /= LevelSizeMultiplier`。
- 默认 `BaseLevelSize=10MB`、`LevelSizeMultiplier=10`,所以 L6=10MB、L5=100MB、L4=1GB… 上层数据逐级放大。
- 文件大小:L0 = `MemTableSize`;≤ dstLevelId 用 `BaseTableSize=2MB`;之上每层 `*= TableSizeMultiplier=2`。
- **优化点**:若 dst 层是空的、且下一层装得下,`dstLevelId++` 让数据**一次沉到底**,减少无用压缩层数。

**追问**:为什么要让深层文件更大?→ 文件大 → 层内文件数少 → 索引小、层内二分成本低、压缩时元数据开销摊薄;浅层文件小是为了让压缩粒度细、写放大低。

---

### Q25. 你实现的"三条压缩路径"分别是哪三条?各自何时触发?

**考察点**:简历里写了"三条压缩路径",这是必被追问的点。

**参考答案**:
`doCompact` 按来源层分流:
1. **L0 → Ldst / L0 → L0**:`findTablesL0ToDstLevel` 与 `findTablesL0ToL0`。L0 因为 key 重叠,压缩要选**一组**重叠表一起归并。
2. **Ln → Ln+1**(`findTables`):按 `sortByMaxVersion` 升序(版本低的先清理),先找与下一层**无重叠**的表——无重叠时可以直接下移,一次 IO 都不用重写;否则带上重叠的 nextTables 一起归并。
3. **Lmax → Lmax**(`findMaxLevelTables`,末层内回收):`sortByStaleDataSize` 降序,只选满足条件的表:`MaxVersion <= discardTs`、表龄 ≥ 1h、stale ≥ `LevelMaxStaleDataSize=10MB`。文件够大直接选,否则 `collectNextTables` 合并相邻表凑够 `fileSize`。

**追问**:为什么末层需要一个特殊路径?→ 末层没有下一层可下沉,但**删除标记和被覆盖的旧版本会永远留在末层**,造成空间放大。所以末层必须能"原地重写"回收 stale 数据,否则磁盘只会涨不会跌。

---

### Q26. 压缩的触发条件(分数)怎么算?

**考察点**:调度逻辑。

**参考答案**:
`pickCompactLevels()` 给每层算分:
- **L0**:`score = numTables / NumLevelZeroTables`(默认 5 张表满分);
- **Ln**:`score = (totalSize - delSize) / levelTargetSSize[i]`;
- **adjusted 比值下探**:若上一层 adjusted > 1,当前层除以它,避免"上一层已经积压时继续在下面猛压";`score < 0.01` 用 0.01 兜底;
- **过滤**:保留 `score >= 1.0` 的层,但 `levelId == 0` **永远参与**(保证 L0→L0 小表合并能被调度到);按 adjusted 降序排序。

`RunOnce` 每轮取分,compactorId==0 时额外 `moveL0toFront`,然后 `prio.adjusted < 1.0` 就 break。

**追问**:调度周期是多少?为什么是 50s?→ `runCompacter` 用 `ticker := 50000ms`,每个 compactor 启动时随机延迟 0–1s 错峰。50s 是个偏懒的取值,好处是稳态开销低,坏处是突发写入时 L0 积压响应慢——这是我在 `comparison.md` 里主动承认的短板。成熟做法是事件驱动(flush 完触发 L0 检查)+ 分数驱动,而不是纯定时器。

---

### Q27. L0→L0 这条路径为什么存在?

**考察点**:是否能解释一个看起来"多余"的设计。

**参考答案**:
L0 表数直接影响读放大(L0 读要线性扫全部表)。如果 L0 里有很多**小表**,那么:① 每次 L0→Ldst 压缩都要带上它们,写放大高;② 读时要一张张扫,延迟差。L0→L0 的作用是把 L0 内的小表**就地合并成大表**,条件(见 `findTablesL0ToL0`):
- 仅 `compactorId == 0`(单一路径,避免重复);
- 跳过 `Size >= 2*fileSize[0]` 的(已经够大)、表龄 < 10s 的(刚 flush 出来的别急着动)、已在压缩中的;
- 候选少于 4 张返回 false;
- `cd.thisRange = keyRange{inf:true}`(强制全重叠),`cd.dst.fileSize[0] = math.MaxUint32`(合成尽量大的文件,最大约 4GB)。

**追问**:这条路径有个众所周知的故事?→ 是的,`lsm/compact.go` 里判断 build 目标是哪一层时一度用了**目标层号**而不是**源层号**,导致 L0→L0 永远不会被调度——这是我修的"三条压缩路径死代码"之一(见 Q56)。

---

### Q28. 末层回收怎么判断一张表"值得重写"?

**考察点**:空间放大的治理。

**参考答案**:
末层表的 `TableIndex.StaleDataSize` 持久化了"表内已死数据字节数",`SstBuilder.AddStaleKey` 在构建时累加。`findMaxLevelTables` 用三个条件筛:
- `MaxVersion > discardTs` → 有活跃读者可能还在用,跳过;
- 表龄 `< 1h` → 刚写不久,先不动;
- `stale < LevelMaxStaleDataSize(10MB)` → 回收收益不够,跳过。

满足条件后按 `staleDataSize` 降序选,文件够大直接选,否则合并相邻表凑够 `fileSize`,最终一次重写把 stale 数据真正丢掉。

**追问**:stale 数据怎么产生的?→ 主要是两类:被更新覆盖的旧版本、以及落在末层的删除标记。删除标记在末层**不能直接删**(删了就可能让更浅层的旧值"复活"),所以先记成 stale,等这条路径确认没有重叠后一起清。

---

### Q29. 多个 compactor 并发跑,怎么避免它们压同一批表?

**考察点**:并发控制,细节题。

**参考答案**:
用 `compactIngStatus`(`compact.go`):每层维护已登记的压缩区间(`ranges`)+ 正在压缩的表集合(`tables map[fid]`)。`compareAndAdd` 在锁下检查新区间与已登记区间**是否重叠**,重叠则拒绝本次压缩(`overlapsWith`);收尾 `deleteCompactionDef` 释放登记。这样两个 compactor 不会同时碰同一批表,也不会压出范围交叠的下一层表。

压缩内部还有并行:`addSplits` 按 `width = ceil(len(nextTables)/5)`(最小 3)把 nextRange 切成 ≤5 段,`compactBuildTables` 每段一个 goroutine,用 `utils.NewThrottle(8+len(splits))` 限制在跑的 builder 数,结果经 `res chan *Table`(cap 3)收集,`SyncDir` 后按 `MaxKey` 排序。

**追问**:为什么用 Throttle 而不是直接限制 goroutine 数?→ Throttle 是**运行时动态抢令牌**(`Do`/`Done` 配对),可以让"某些 subCompact 提前结束、某些还在跑"的情况下保持并发度;纯粹用固定 worker 池不如这个灵活。注意 `Finish()` 里 `wg.Wait` 后再 close channel 的顺序不能错。

---

### Q30. 压缩时怎么决定哪些旧版本可以删?为什么是 `version <= discardTs`?

**考察点**:MVCC 安全性的核心论证,高频题。

**参考答案**:
判据(`compact.go` 的 `addKeys`):
- `discards = lm.getDiscardTs()`,即 `Option.TxnDoneIndex`——`startMark` 发布的"已完成水位",含义是**所有 startTs ≤ discards 的读者都已结束**;
- 只有 `version <= discards` 的版本才可能被回收;配合 `NumVersionsToKeep=1`,同 key 保留最新一版;
- 若条目是删除/过期墓碑,或已经保留够版本数,则 `skipKey` 跳过同 raw key 后续所有版本;
- 保留判定:`!expired && lastKeyVersion` 保留最新版;`hasOverlap || keepForLastLevel` 保留(有重叠或末层墓碑);否则丢弃并计入 discardStats。

**安全性论证**:一个 `startTs = T` 的读者必须能看到所有 `version ≤ T` 的版本。若某个 `version ≤ T` 被删而该读者还在读,就会读到"历史被篡改"。`discards` 是所有活跃读者的最大已完成 startTs,所以删 `version ≤ discards` 绝不会影响任何活跃读者。

**关键细节**:**压缩开始前就快照 discardTs**(`compact.go:594` 的注释)。因为一次压缩可能跑几秒,期间水位会推进,如果同 key 的前后版本按不同水位判定,可能保留出**版本号比被删版本还旧**的结果,破坏不变量。

**追问**:为什么是"≤ discards"而不是"< minActiveStartTs"?→ 语义等价但表述不同:`discards` 就是"所有活跃 startTs 的下界里已确认安全的那部分",LimitMark 保证 ≤ discards 的 index 不再有活跃读者。

---

### Q31. 归并迭代器(MergingIterator)怎么工作?为什么它不做 SafeCopy?

**考察点**:多路归并的实现。

**参考答案**:
- `LSM.NewLsmIterator` 持读锁取 mutable/immutable memtable 的跳表迭代器,再加 `LevelManger.iterators`:L0 每个表一个 `TableIterator`(逆序,新表在前),Ln 用一个 `ConcatIterator`(按 MaxKey/MinKey `sort.Search` 定位表)。
- `MergingIterator` 用 `container/heap`: `Less` 先按 `Valid()`(有效在前)再按 `CompareKeyWithTs` 升序;`Next` 就是 `heap.Pop` 最小迭代器 → `Next()` → 有效则 `heap.Push`。因为 key 编码是 (rawKey asc, ts desc),归并后**同 raw key 的最高版本最先出**。
- `iters []interfaces.Iterator` 额外保存原始子迭代器,因为堆会被耗尽。
- **`Item` 直接返回堆顶,不做 SafeCopy**(`lsm_iterator.go:219` 注释):依赖子迭代器的 Item 在有效期内稳定。跳表迭代器的 key/value 是 arena 视图(永久有效),SST 迭代器的 key 走 `TableIterator` 自带的 64KB `ChunkedArena`,在 Rewind 前也稳定。

**追问**:为什么迭代器需要 chunked arena?→ 这是"零分配扫描"的一部分:逐 key 做 `SafeCopy` 会产生一次分配,10 万条扫描就是 10 万次;改成按块分配、块内游标推进,把分配次数降到与块数同阶(见 `utils/chunkedArena.go`,默认 64KB 一块,块一经分配不移动,返回切片的生命周期可预测)。

---

### Q32. 事务迭代器的 MVCC 可见性和"读己之写"在迭代器上怎么实现?

**考察点**:迭代器与事务的结合,容易被忽略。

**参考答案**:
- `newPendingWritesIterator` 把事务 `pendingKeys` 的所有 entry 取出、按 key 排序,和 LSM 各层迭代器一起喂给 `NewMergingIterator`。
- `pendingWritesIterator.Item()` 把 key 重写成 `KeyWithTs(key, startTs)`、`Version=startTs`——**保证 pending 版本在合并序里位于 LSM 旧版本之前**(因为 ts 大 → 编码小 → 排在前面)。
- `TxnIterator.Item`(`db.go`)做三件事:① 若 pending 命中,用 pending 的 entry 覆盖;② `IsVisible` 过滤(`startTs >= ParseTsVersion`);③ 同 raw key 多版本去重(`lastKey`),只返回第一个可见版本;`BitDelete` 的墓碑返回 `ErrKeyNotFound` 语义。

**追问**:`pendingWritesIterator` 保证了顺序,但为什么 `Item` 还要再覆盖一次?→ 因为 LSM 里可能存在**同 key 的旧版本**排在 pending 后面,去重逻辑(`lastKey`)虽然能过滤,但首个可见条目必须来自 pending;覆盖是显式保证这一点,而不是依赖排序不出错。

---

# 第三层:事务、MVCC 与并发(Q33–Q52)

### Q33. `startTs` 和 `commitTs` 分别怎么分配?是不是同一个时间戳?

**考察点**:TSO 基本功。

**参考答案**:
来自全局单调计数器 `nextTxnTs`(`tsLock` 保护),**没有物理时钟**:
- `startTs()`:读锁下 `startTs = nextTxnTs - 1`,即"发号时能看到的最大已提交号"。它同时是**读快照上界**和**自己的写版本号**(`modify` 里 `entry.Version = startTs`);
- `newCommitTs()`:写锁下 `commitTs = nextTxnTs; nextTxnTs++`。提交时才领取。

所以 `startTs ≠ commitTs`,通常 `commitTs > startTs`(并发提交会拉开差距)。开库时从 `db.MaxVersion()` 续号(`Open` 里 `nextTxnTs = MaxVersion()`、`initStartTs`、`commitMark.SetDoneIndex`),保证重启后不复用旧时间戳。

**追问**:代码里有 `readTs` 这个字段吗?→ **没有**。`readTs` 在概念上等于 `startTs`——它是"读视图的上界"。说"startTs 与 readTs 分离"是不准确的,准确的说法是"**读视图锚定(startTs)与写版本领取(commitTs)在时间上分离**"。

---

### Q34. 为什么读事务开启时要等水位?

**考察点**:快照一致性的必要性。

**参考答案**:
`NewTransaction(false)` 纯读事务立即 `commitMark.WaitForIndexDone(startTs)`。原因:提交顺序与时间戳顺序一致——`commitTs` 是全局递增的,但**分配了 commitTs 不等于数据已经写完 WAL/跳表**(commit 是异步的,`SendToWriteCh` 之后由写通道落盘)。如果读事务不等,它可能看到 `startTs` 之下的"已分配 commitTs 但还没落盘"的数据缺失,或者反过来,在扫描过程中一批提交中途落盘造成**同一快照读到不一致的中间态**。

等 `commitMark` 的水位推进到 `startTs`,意味着"所有 commitTs ≤ startTs 的事务都已完整落盘",此后本次读看到的是一个**静止的、完整的快照**。

**追问**:那"提交"到底算不算完成?→ 分两步:事务对象层面 `Commit` 在 `req.Wait()` 之后才 `doneCommit`——**只有数据真的落进 LSM 了,才推进 commitMark 水位**。所以水位推进 = 数据可见,这个等式成立。

---

### Q35. 写事务为什么可以"延迟等待"?`ensureStartTsReady` 解决什么?

**考察点**:对关键性能设计点的理解。

**参考答案**:
看 `Transaction` 的字段 `startTsReady`:
- 纯读事务:`NewTransaction` 时 `startTsReady = !update = true`,在 `startTs()` 内部就等 `commitMark`;
- 写事务:`startTsReady = false`,**直到首次读 LSM**(`Get` 里调 `ensureStartTsReady`)才等。

为什么能延迟?因为**纯写事务不需要读快照**——它只需要自己的 `commitTs`(由 `newCommitTs` 保证),写集的冲突检测也不依赖"看到的数据完整"(读集为空时 `hasConflict` 直接返回 false)。

效果:纯写事务完全不付这笔等待,写延迟显著降低;混合事务只在真正读了 LSM 之后才付。这个设计配合 `startTs` 统一走读锁,是"写锁请求减半"之外的第二个写路径优化。

**追问**:如果写事务先 Set 再 Get 呢?→ 先查 `pendingKeys`(读己之写)命中就直接返回,**不触发** `ensureStartTsReady`——只有真的下探 LSM 才等。这是顺带的收益。

---

### Q36. 为什么所有事务的 `startTs` 都能用读锁?"写锁请求减半"是怎么来的?

**考察点**:锁粒度优化的一个漂亮案例(commit cb30bfe)。

**参考答案**:
关键观察:**`startTs()` 只读 `nextTxnTs`、不改它**,唯一递增 `nextTxnTs` 的是 `newCommitTs()` 和 `incrementNextTs()`,这两个走写锁。所以:

```
startTs():            tsLock.RLock()  →  startTs := nextTxnTs - 1  →  tsCounter 引用计数+1  →  RUnlock()
newCommitTs()/提交:    tsLock.Lock()   →  定 ts / 递增 nextTxnTs   →  Unlock()
```

- 改之前:每个写事务开事务要拿写锁(因为它要"预留"自己的写版本),提交再拿一次 → 每事务 2 次写锁请求;
- 改之后:开事务统一读锁,写锁只在提交时拿一次 → **写锁请求减半**,并发开事务不再串行化。

**有一个顺序性约束不能破**:`tsCounter` 计数 0→1 时触发的 `startMark.Begin(ts)` **必须在持有读锁期间发送**。因为只要锁没释放,任何提交都无法推进 `nextTxnTs`,所以水位推进的 FIFO 通道里不会出现"越过一个尚未登记的 ts"的情况。如果把 Begin 挪到锁外,就可能出现水位跳过一个未登记的读者 → 旧版本被误删。

**追问**:为什么 `curCnt` 用 `atomic.Pointer[tsCounter]` + `panic` 校验?→ 这个是引用计数器与 `nextTxnTs` 的**不变量校验**:`startTs` 读到的 ts 必须与当前 `curCnt` 的 ts 一致,不一致说明有人改了发号逻辑,dev 阶段直接 panic 暴露。另外这里用 `fmt.Sprintf` 而非 `CondPanicf`,注释说明是因为变参 `...any` 会装箱逃逸,热路径每 op 一次分配。

---

### Q37. `tsCounter` 的引用计数解决什么问题?纯读场景为什么能零水位消息?

**考察点**:一个精巧的优化。

**参考答案**:
`tsCounter{ts, refs atomic.Int64}` 的语义:**inc 从 0→1 才发 `startMark.Begin(ts)`,dec 从 1→0 才发 `startMark.Done(ts)`**。

- 因为 `startTs = nextTxnTs - 1`,**所有在当前时刻开启的事务共享同一个 ts**。如果每个事务都发 Begin/Done,水位消息是 2 条/op;
- 加引用计数后,只有当某个 ts 的**第一个**读者出现/最后一个读者离开时才发消息 → 从 "2 条/op" 降为 "2 条/ts";
- **纯读场景**(所有事务共享同一个 ts、且不断有读者)引用计数永远不归零,稳态下**零水位消息**,`markCh` 和 `processOn` 彻底退出读热路径。

**追问**:为什么需要 `retired` map?→ ts 会随时间前进,旧 tsCounter 要"退役"。`doneStart` 先读 `curCnt`,ts 不匹配再去 `refMu` 保护下的 `retired` 里查;`retireStartTs` 的注释明确:**必须先挂 retired、再发布新计数器**,保证 dec 侧无论读到哪个指针都能找到对应计数,否则计数会丢。

---

### Q38. 读写冲突检测是怎么做的?

**考察点**:乐观并发控制(SSI-lite)。

**参考答案**:
`DetectConflicts=true` 时:
- **写集**:每次 Set 把 key 的哈希登记进 `conflictKeys`(双哈希,两个都登);
- **读集**:每次 Get 未命中 pendingKeys 时把 **hash1** 记进 `readKeys`(即 `addReadKey`);
- **提交时** `hasConflict`:遍历全局 `commitedTxns`(已提交事务列表),跳过 `txn.startTs >= commit.TxnCommitTs` 的(自己开始前就提交的,无关),然后看"我读的 key"与"它写的 key"有没有交集,有则返回 `ErrConflict`。

这是**快照校验式**的乐观并发:提交时验证"我读过的数据在我开始之后有没有被别人改过"。

**追问**:`readKeys` 为空为什么直接放行?→ 没读过任何东西 → 不可能有读写冲突,直接 `return false`。这也是**纯写事务几乎零冲突检测开销**的原因。

---

### Q39. 为什么写 key 要登记两个哈希?

**考察点**:哈希碰撞的误报问题。

**参考答案**:
`utils.KeyToHash` 返回两个独立的 64 位哈希:Go 运行时的 `MemHash` 和 `xxhash.Sum64`。两个都登记进 `conflictKeys`。

目的是**消除单哈希碰撞导致的误报冲突**:如果只用一个 64 位哈希,两个不同的 key 有约 2^-64 的概率碰撞,一旦碰撞,两个本不相干的写就会被误判成冲突,事务无谓地 `ErrConflict` 重试。用两个独立哈希后,同时碰撞的概率可忽略。

**注意区分**:`pendingKeys` 这个 map **只用 hash1** 作键(省 `string(e.Key)` 的分配),碰撞由读取时的 `bytes.Equal` 兜底校验真实 key;`readKeys` 也只记 hash1。所以双哈希只用于 `conflictKeys`,而且它是**精确性换内存**:冲突检测宁可多存 64 位也不误报。

**追问**:碰撞会导致数据错误吗?→ 不会。`pendingKeys` 的碰撞会被 `bytes.Equal` 拦下,最多是极低概率的一次"写到别人的槽位"被校验拦住(理论上会丢失该次写),但概率低到工程上可忽略且不会静默返回错误数据。真正需要注意的是**冲突检测的漏报**:`readKeys` 只用 hash1,漏报也是 2^-64 量级。

---

### Q40. 两个事务都写同一个 key 但都没读过它,能检测到冲突吗?

**考察点**:推到边界,考察是否真懂乐观并发。

**参考答案**:
**检测不到**。`hasConflict` 的比较是"我的 `readKeys`" ∩ "别人的 `conflictKeys`"。两个事务都只写不读,`readKeys` 为空,`hasConflict` 直接返回 false。结果是**后提交者覆盖先提交者(last-writer-wins)**,这是本实现明确的语义选择,不是 bug。

但如果事务**先 Get 再 Set 同一个 key**,读集就包含该 key,写写冲突也能被识别(因为我的读集里有它,对方的写集里有它)。

**追问**:这对用户意味着什么?→ 对同一 key 的"读-改-写"必须复现真实的读操作(先 Get 再 Set),才能拿到冲突保护。想做成"任何写写都冲突"需要额外的写集互斥(如 key 级锁或两阶段提交),代价是并发度下降。这是**文档里应该写清楚的一致性契约**,我在 `comparison.md` 里把它列为已知局限。

---

### Q41. `commitedTxns` 会无限增长吗?水位怎么回收它?

**考察点**:内存回收的正确性论证。

**参考答案**:
不会,靠 `cleanCommitedTransaction`:
```
maxStartTs := startMark.GetDoneIndex()   // 所有活跃事务中最小的 startTs 的已完成水位
if maxStartTs == lastCleanupTs { return }
for _, txn := range commitedTxns {
    if txn.TxnCommitTs <= maxStartTs { continue }   // 可以安全回收
    tmp = append(tmp, txn)                          // 保留
}
commitedTxns = tmp
```
论证:任何 `TxnCommitTs <= maxStartTs` 的历史提交,其 commitTs 早于或等于**当前所有活跃事务的 startTs**,在 `hasConflict` 里必然命中 `txn.startTs >= commit.TxnCommitTs` 而 `continue`——它永远不可能再造成冲突,所以可以回收。

`lastCleanupTs` 保证水位单调(`AssertTrue(maxStartTs >= lastCleanupTs)`);回收用 `tmp := commitedTxns[:0]` 复用底层数组,不额外分配。

**追问**:清理时机在哪个锁下?→ 在 `newCommitTs` 的**写锁内**调用(提交时顺带清理)。所以它不会和发号并发,读 `startMark` 水位是原子的。

---

### Q42. `LimitMark` 和 `AtomicMark` 有什么区别?为什么需要两个?

**考察点**:两个水位结构的存在理由。

**参考答案**:
语义不同:

| | `LimitMark`(startMark) | `AtomicMark`(commitMark) |
|---|---|---|
| 回答的问题 | 谁还在读(水位 = 旧版本何时可回收) | 谁提交完了(读可见性) |
| Begin/Done 关系 | **同一 index 可被多次 Begin**(多个事务共享 ts,靠引用计数合并) | Begin/Done **一一对应**(一个 ts 一个提交) |
| 水位推进 | 需要 begin/done 计数 + 最小堆(必须知道每个 index 有几人没走) | 顺序完成走 CAS 快路径,只需知道"下一个是不是我" |
| 用途 | 压缩回收旧版本(`getDiscardTs`) | 读事务等待快照完整(`WaitForIndexDone`) |

一个 ts 可以被多个读事务共享(引用计数),所以 startMark 必须支持一个 index 多个 begin;而 commitMark 的 index 是提交序列,天然一一对应。用一个结构硬扛两种语义会很别扭,所以分开实现。

**追问**:两个结构各自的复杂度风险在哪?→ 都在**等待者唤醒**上,因为"水位一次连跳多级"和"乱序压堆"都会让等待者漏唤醒 → 死等。见 Q45、Q46。

---

### Q43. `LimitMark` 是怎么实现的?

**考察点**:并发数据结构实现细节。

**参考答案**:
- 一个 `markCh chan mark`(容量 **8192**)+ **单消费者协程** `processOn` 串行化所有水位事件——串行化是正确性的关键,不需要在推进逻辑里加锁;
- `processOne` 维护 `indexDoneNum map[uint64]int`(begin 数 - done 数),用**手写的 `u64Heap` 最小堆**找当前最小未完成 index;只有堆顶的 dones > 0 才能推进水位,循环推进到不能推为止;
- 推进用 `CompareAndSwap(doneIndex, curIndex)`,并 `doneIndexMirror.Store(curIndex)` 发布给 compactor **原子读**(无通道、无通知协程,避免 compactor 被唤醒风暴);
- 等待者:`indexWaiters map[uint64][]chan struct{}`,`notifyAndRemove` 关闭等待者的 channel;遍历有"密集 vs 稀疏"两套分支优化。

为什么手写堆?注释写得很清楚:`container/heap` 通过 `interface{}` 会**装箱逃逸**,提交热路径每记录 2–3 次分配,而水位在热路径上。这是典型的"库很对但场景不对"。

**追问**:`markCh` 为什么从 100 提到 8192?→ `Begin/Done` 是**持 `tsLock` 读锁**发送的,如果通道满就会阻塞,进而**放大 tsLock 的排队**。8192 个 mark 每条约 48B,约 400KB,换取排队不溢出。

---

### Q44. `AtomicMark` 的快路径和慢路径分别是什么?

**考察点**:无锁 + 兜底的混合设计。

**参考答案**:
- **快路径**(顺序完成,最常见):`Done(x)` 发现 `x == doneIndex + 1`,直接 `CompareAndSwap(doneIndex, d, x)` 成功 → `absorb()` 连推水位(处理堆里已积压的连续 index)→ `wakeWaiters(最终水位)`;
- **慢路径**(乱序完成):`x != doneIndex + 1`,加锁把 x 压进 `pending` 最小堆,`heapLen` 原子更新;然后**必须再 `absorb()` 一次**,最后唤醒。

`absorb` 和 `wakeWaiters` 都有零开销快速返回:`heapLen == 0` / `waiterCnt == 0` 时直接 return,所以稳态下没有等待者、没有乱序时,`Done` 就是一次 CAS。

**追问**:为什么 `Done` 里 CAS 成功后还要 `absorb`?→ 因为可能出现"我推进到 x 之后,堆里其实已经积压了 x+1, x+2…"的情况(它们之前乱序进来过)。不 absorb 的话水位会卡住,等下一个 Done 来推——如果那是最后一个 Done,就永远卡住。

---

### Q45. `AtomicMark` 修过的死锁之一是什么?

**考察点**:并发 bug 的故事题,面试官很爱问。

**参考答案**:
**水位连跳后漏唤醒**。快路径 CAS 成功后 `absorb()` 可能把水位从 x 连推到 x+n。如果仍然用**吸收前的 x** 去调 `wakeWaiters`,那么等待 index ∈ (x, x+n] 的事务就永远收不到 channel close;更要命的是,负载末尾**不会再有新的 Done 来补救** → **事务永久阻塞**。

线上表现非常有辨识度:混合负载 operationcount 快跑完时,`Count` 突然冻结,进程还在、CPU 0%——不是 crash,是所有人都在等一个永不到来的唤醒。

修复:`m.wakeWaiters(m.GetDoneIndex())`——用**吸收后的最终水位**唤醒(见 `utils/atomicMark.go` 的 `Done`)。回归测试 `TestAtomicMarkWakeAfterAbsorb`。

**追问**:第二个死锁呢?→ Q46。

---

### Q46. `AtomicMark` 的第二个死锁是什么?为什么"压堆后要再 absorb 一次"?

**考察点**:竞态窗口分析能力。

**参考答案**:
**压堆路径漏吸收**。竞态窗口:
1. 慢路径协程读到 `doneIndex = d`(在快路径 CAS 之前);
2. 快路径协程 CAS 到 x、`absorb()`——此时堆是空的(慢路径还没 push);
3. 慢路径协程这时才 `push(x)` 进堆并解锁。

如果压堆后不重新 `absorb()`,x 就**永远留在 pending 堆里**,水位停住,不会再有 Done 推动 → **所有 `WaitForIndexDone` 死等**。

线上表现:4 个 worker 全部卡在 `commitMark.WaitForIndexDone`,CPU 0%。

修复:`m.mu.Unlock()` 之后**补一次 `m.absorb()`**(`atomicMark.go`)。回归测试 `TestAtomicMarkConcurrentDone`。

**追问**:这类 bug 的通用规律是什么?→ **"检查—行动"之间状态被外部改变**。快路径是"检查堆空 + 行动(CAS)",但它检查和行动不是在同一个锁内,所以慢路径可以在窗口里插入。修法有两种:① 慢路径做完自己的事后再重新检查一次(本项目的选择,快路径保持无锁);② 快慢路径共用一把锁(牺牲性能)。我选了性能,代价是必须显式补吸收。

---

### Q47. 线上遇到"进程活着、CPU 0%、吞吐冻结",你怎么排查?

**考察点**:排查方法论,工程能力体现。

**参考答案**:
这类症状几乎是**死锁或永久等待**的典型指纹(crash 会有退出码,死循环会 CPU 100%)。排查顺序:
1. **确认是"等"而不是"崩"**:看进程存活、CPU ~0%、日志无异常。本项目两次 `AtomicMark` 死锁都是这个指纹。
2. **拿到所有 goroutine 栈**:Go 的 `SIGQUIT` 或 `pprof/goroutine?debug=2` 会打印全量栈。看**大家的调用栈是否汇聚到同一个等待点**——本项目是全部汇聚在 `commitMark.WaitForIndexDone`,这就把范围缩到了水位。
3. **从等待点反推不变量**:`WaitForIndexDone(x)` 要的是"水位推进到 x"。那就问:水位由谁推?谁会唤醒我?什么情况下这个唤醒不会发生?→ 引出"连跳后漏唤醒"和"压堆后漏吸收"两个窗口。
4. **构造最小复现**:把并发时序写成一个单元测试(`TestAtomicMarkWakeAfterAbsorb` 人为制造连跳、`TestAtomicMarkConcurrentDone` 制造压堆竞态),让修复前的代码**稳定失败**。这是最重要的一步——**没有能稳定抓住 bug 的测试,就不知道修没修好**。
5. **回归**:`-race` 全量 + 额外 `-count` 重复跑。

**追问**:为什么"测试能稳定复现"这么关键?→ 因为它把"我觉得是这样"变成"证据表明是这样"。我在 Q49 的幽灵 key 问题上也是同一套路:先写出能失败的测试,再动代码。

---

### Q48. 事务对象池化时,哪些字段要重置、哪些可以复用?

**考察点**:`sync.Pool` 的正确用法。

**参考答案**:
`NewTransaction` 从 `db.txnPool.Get()` 取对象,**必须显式复位**(注释写明"取出时必须复位,否则带回上次残留"):
- 必须清:`db`、`update`、`count=1`、`size=len(TxnKey)+10`、`commitTs=0`、`discard=false`、`startDone=false`、`startTsReady=!update`、`readKeys=nil`、`keyBufs=keyBufs[:0]`、`numIterators.Store(0)`。
- 可以复用(不重建,降 GC):`pendingKeys` map(`clear` 清空保留底层桶)、`conflictKeys` map、`entries []*model.Entry`(提交时 `t.entries[:0]`)、`finEntry`(finTxn 的 entry scratch)、`finTsBuf []byte`(`strconv.AppendUint(t.finTsBuf[:0], ...)`)、`keyBufs`。

**一个关键前置条件**:`conflictKeys` 能被安全复用,是因为提交时已经把它**快照成 `[]uint64`** 存进 `commitedTxns`(`commitedTxn{conflictKeys []uint64}`)——这样 map 归池后,冲突检测比较用的还是那份快照,不会被下一个事务的写覆盖。这个设计是"map 复用"能成立的前提。

**追问**:为什么 `plan`/pool 里的 map 不直接 `make` 新的?→ `NewTransaction` 曾经占 pprof 的 **34.6%**——每事务一个结构体 + 两个 map 共 3 次分配。池化后这部分几乎归零。

---

### Q49. 点查和迭代器分别怎么实现"读己之写"?

**考察点**:两个入口的一致性。

**参考答案**:
- **点查**(`txn.Get`):先 `pendingKeys[MemHash(keyNoTs)]` 命中且 `bytes.Equal` 校验真实 key → 直接返回副本(`Version = startTs`);若 pending 是删除/过期 → 返回 `ErrKeyNotFound`;**未命中才** `addReadKey` 登记读集并下探 LSM。注意顺序:先 pending 再 LSM,保证自己刚写的删除不会被 LSM 里的旧值"覆盖回来"。
- **迭代器**:见 Q32——把 pending 做成一个虚拟迭代器,key 重写成 `KeyWithTs(key, startTs)` 参与归并,天然排在同 key 的 LSM 旧版本之前。

**追问**:为什么点查用 `MemHash(key)` 作 map 键而不是 `string(key)`?→ 省一次字符串分配。碰撞用 `bytes.Equal` 兜底。这是一致的两级策略:哈希定位、精确校验。

---

### Q50. `RollBack` 和 `Discard` 的区别?这里有坑吗?

**考察点**:是否真的读懂了生命周期,敢不敢指出自己的问题。

**参考答案**:
- `Discard`:幂等;置 `discard=true`;调 `doneStart` 归还时间戳引用;遍历 `pendingKeys` 全部 `Release`;`clear` 两个 map;把 `keyBufs` 逐个 `KeyTsBufPool.Put`;最后 `db.txnPool.Put(t)` 归还对象池。
- `RollBack`:只释放 `pendingKeys` 的 entry,置几个字段为 nil。**不调 `doneStart`、不 `Discard`、不归还 txnPool**。

`Commit` 用的是 `defer Discard()`,所以正常路径没问题。但**如果调用方只调 `RollBack`**,会造成 `tsCounter` 引用计数泄漏(水位推不动,旧版本回收被卡住),并且事务对象不归还池。

**我的处理**:这是我在 review 时发现的不一致,当前实现里内部路径都走 `Discard`,所以没有实际触发;但 API 语义上 `RollBack` 应该等价于"放弃这个事务",最稳妥的做法是让 `RollBack` 内部也走 `doneStart` 或直接改为 `Discard` 的别名。**面试时主动指出这类"还没爆炸但语义不一致"的点,比被问住强**。

**追问**:还有类似的"看起来没用"的字段吗?→ 有。`Transaction.numIterators` 只在 `NewTransaction` 置 0、`Discard` 里检查,真正的迭代器计数在 `vlog.incrIteratorCount/decrIteratorCount`。这是重构留下的痕迹,应该删掉或接上,属于我列在 `todo-list.md` 里的清理项。

---

### Q51. `writeChLock` 保护什么?为什么 `req.Wait()` 要放在锁外?

**考察点**:锁粒度。

**参考答案**:
`writeChLock` 只在 `commitAndSendToDB` 里保护"**定 commitTs + 构建批次 + 下发 writeCh**"这一段。它保证提交序与时间戳序一致:定 ts 和把请求放进通道是一个原子动作,不会出现"A 拿到更小的 commitTs 但比 B 晚进通道"从而乱序落盘。

`req.Wait()`(阻塞等写盘完成)**放在释放锁之后**——注释明确"释放了锁后,阻塞等待 lsm 结果"。原因:Wait 可能等数毫秒(fsync 场景),持锁等待会把所有并发提交串行化成"一个提交等完下一个才能定 ts",吞吐直接退化。所以是"两段式":锁内定序 + 锁外等待。

**追问**:那 `Commit` 的返回意味着什么?→ 意味着一批数据真的落进 LSM 了(`doneCommit` 已推进 commitMark)。所以**提交返回即持久**在 `SyncWrites=true` 下成立;`SyncWrites=false` 下"返回"只表示进了 memtable/WAL buffer,掉电可能丢最后一段——这是默认配置的已知语义,必须跟用户讲清楚。

---

### Q52. 你项目里的锁有哪些?怎么定锁顺序?

**考察点**:全局视角的并发控制。

**参考答案**:
| 锁 | 保护对象 |
|---|---|
| `LSM.sync RWMutex` | `memoryTable` 指针、`immemoryTables` 切片 |
| `LevelHandler.mux RWMutex` | 该层 `tables/totalSize/totalStaleSize` |
| `compactIngStatus.mux RWMutex` | 压缩区间与表集合 |
| `ManifestFile.mux Mutex` | MANIFEST 内存态与文件追加 |
| `TransactionManager.tsLock RWMutex` + `writeChLock Mutex` | 发号与提交串行 |
| `WAL.lock Mutex` | WAL 写偏移 |
| `Table.ref int32`(原子) | SST 生命周期 |
| `VLogFile.Lock RWMutex` + `filesLock` | vlog 读与扩容/删除 |
| `cacheShard.m RWMutex`(16 把) | 块缓存分片 |

锁顺序的经验规则:**从"更全局"到"更局部"**——`LSM` 层锁 → `LevelHandler` 锁;`filesLock` → `VLogFile.Lock`(先定位文件、再锁文件);绝不反向获取。另外有几个**刻意的顺序保证**:`retireStartTs` 先挂 retired 再发布新计数器;`LimitMark.Begin` 必须在 `tsLock` 读锁内发送。

**追问**:怎么发现锁顺序错误?→ 死锁往往在压力测试下偶发。实践手段:① `-race` 只能抓数据竞争、抓不到死锁;② 压测 + 超时检测;③ Go 运行时死锁检测(所有 goroutine 都睡了会 panic)只在**全睡**时有效,本项目那种"部分 goroutine 永久等待"不会触发,所以靠 Q47 的栈分析。

---

# 第四层:工程落地:bug、性能、测试(Q53–Q76)

### Q53. group commit 完整讲一遍

**考察点**:简历"group commit 攒批摊销 fsync"必被追问。

**参考答案**:
在 `handleWriteCh`(单消费者协程):
- 通道 `writeCh` 容量 `KvWriteChCapacity=1000`;攒批阈值 `WriteChBatchThreshold=900`(必须小于容量);
- 外层拿一个请求后,内层用 `select + default` **非阻塞排空**通道,聚到 900 为止;
- **fsync 汇合窗口**(仅 `SyncWrites=true` 且本批请求数 > 1 时):用复用 `time.Timer` 等 `SyncGroupCommitWait=1ms`,期间继续吸收并发提交,减少同步次数;串行提交(本批和上批都只有 1 个请求)不等待,避免无谓延迟;
- **串行化写盘不用条件变量,用容量 1 的令牌 channel `blockChan`**:持有者直接 `WriteRequest`(零额外协程),抢不到则 `go writeRequest(reqs)` 排队,进函数先 `blockChan <- struct{}{}` 再 `defer <-blockChan`;
- 批量刷盘:`WriteRequest` 成功后再 `db.Lsm.SyncWalFile()`(WAL 只同步新增前缀,见 Q58);
- 失败边界:`doneRange(err, from, to)` 把失败点**及之后**的请求标记失败,之前已完整写入的标记成功,避免重试产生重复版本。

**追问**:为什么不用 `sync.Cond`?→ 见 Q54。

---

### Q54. 为什么 group commit 用 channel + 定时器,而不是 `sync.Cond`?

**考察点**:实现选择的合理性。

**参考答案**:
`sync.Cond` 适合"等条件成立",但它有几个不适合这里的点:
1. **批量的边界不清晰**:Cond 的 `Signal/Broadcast` 唤醒后,消费者拿到锁只能看到"有人来了",无法自然地表达"等到通道里没有更多了"或"最多等 1ms";
2. **超时不好做**:Cond 没有超时(Cond 需要配合 `Wait` 与外部 timer,自己管理容易错),而汇合窗口天然是"1ms 或攒够 900,谁先到算谁";
3. **channel 同时充当队列和背压**:请求要通过 channel 从多个提交者传到唯一写者,channel 本身已经是队列;再加一个 Cond 是重复抽象;
4. 项目的整体风格就是"**用 channel 做状态传递、用原子做水位**",减少共享可变状态。

代价是 `select + default` 的排空循环和 timer 的 Stop/排空要写对(我们踩过"提前退出没排空 timer.C"的坑)。

**追问**:1ms 这个值怎么定?→ 是 fsync 延迟与批大小的折中:fsync 约 4ms 量级,1ms 汇合窗口能显著增大批次同时引入最多 1ms 额外延迟。太小摊不动 fsync,太大会让串行提交的尾延迟变差。真实系统应该做成自适应(按到达速率调)。

---

### Q55. "值先于引用落盘"是什么意思?顺序反了会怎样?

**考察点**:崩溃一致性的关键约束。

**参考答案**:
`WriteRequest` 的顺序是:`vlog.Write(reqs)` → `SyncWrites` 时 `vlog.Sync()` → 逐条 `writeToLSM` → `Lsm.SyncWalFile()`。

大 value 在 LSM 里只是一条 **12 字节的 `ValuePtr`**(指向 vlog 的 fid/offset/len)。如果先把指针写进 WAL 并持久化,再去写 vlog,那么"WAL 已持久但 value 还没落盘"的窗口里崩溃 → 重启后 WAL 里有一个指向**不存在或未写完的 vlog 记录**的指针 → 读这个 key 会读到垃圾或报错。

所以必须**先让 value 在 vlog 里持久化,再让指向它的引用(WAL)持久化**——这是所有 kv 分离系统的铁律(和文件系统"先写数据再写元数据"一个道理)。

**追问**:不开启 SyncWrites 时顺序还有意义吗?→ 有。不 sync 只是"系统崩溃会丢最后一段",但**顺序约束是关于"相对次序"的**:即使两者都在 page cache 里,内核回写顺序也不保证。不 sync 时的正确性依赖"崩溃后 WAL 丢了指针也一并丢"这种粗粒度一致性,风险是**部分回写**导致的不一致。所以对持久性有要求的场景必须开 SyncWrites。

---

### Q56. "三条压缩路径死代码"是怎么回事?

**考察点**:简历写了"修复死代码",这是必问的故事题。

**参考答案**:
压缩构建新表时,需要判断"这批数据是要下沉到下一层,还是留在本层(L0→L0)"。原实现用的是**目标层号**(`cd.dst` / `cd.nextLevel` 的 levelID)去判断,但两个场景的目标层号可能都是 0 或者都非 0,导致判断错位——具体表现是 **L0→L0 这条路径永远不会被调度**:调度器 `pickCompactLevels` 会算出候选,但构建阶段的条件永远不满足,直接落回默认分支。

类似的还有三处:`StaleDataSize` 构建时没持久化进 `TableIndex`(末层回收拿不到依据)、删除标记不进末层(墓碑被过早清掉可能导致旧值复活)、`findMaxLevelTables` 选中了大表却不真正压缩(选了但没压)。

修复的关键:判断条件改成按**源层**(`cd.thisLevel.levelID == 0`)区分,并把 `StaleDataSize` 写进索引、墓碑按层保留策略处理。

**追问**:怎么发现的?→ 做压缩路径的**覆盖测试**时发现:我写了 `TestCompactionL0ToL0`、`TestCompactionL0ToLmax`、`TestLastLevelCompaction` 三个定向测试,前一个怎么都进不去目标分支。给每段逻辑加断言/日志看命中的是哪个分支,就定位到了"条件恒假"。**这类 bug 的特征是"代码能编译、测试不报错、只是这条路径永远不走"**——它不会自己暴露,只会让系统悄悄退化(比如末层永远不回收、磁盘只涨不跌),必须靠路径覆盖测试逼出来。

---

### Q57. 读路径最大的一个优化是什么?"2440ns → 1800ns"做了什么?

**考察点**:简历读路径数字的来源。

**参考答案**:
**memtable 命中后白搜 8 层**。基线 `BenchmarkReadGetHitRandom` 是 2367 ns/op,profile 显示 `findNear` 48%、`getKey` 30%,但还有 **`LevelsManger.Get + LevelHandler.Get + searchLnSST` 合计约 25%**——这是纯浪费。

根因:早返回条件是 `entry.Version == startTs`。而 `startTs = nextTxnTs - 1`,**恰好等于最近一次提交的 commitTs** → 只有"最后一批提交"的数据命中 `==`,更早的数据(基准里 99.5%)版本 < startTs,全部掉进全层搜索,最后返回的还是 memtable 里那同一个 entry。

改法:命中且 `Version <= startTs` 就提前返回。量化:`ReadGetHitRandom` 2367 → **1729 ns/op(-27%)**,`ReadGetHitSeq` 1224 → **801 ns/op(-35%)**。

**追问**:为什么这个优化要"过两遍"?→ 因为第一版漏了一个不变量。见 Q58。

---

### Q58. `<=` 早返回为什么需要 `BitTxn` 门控?

**考察点**:优化与正确性的冲突处理。

**参考答案**:
`<=` 早返回依赖三条引理:① 命中即可见(key 编码保证命中的必然 version ≤ startTs);② **memtable 命中即全局最新**(存储层新旧顺序 = 版本顺序);③ 所以全层搜索返回的是同一个 entry。

其中引理 ② 被 **vlog GC 破坏**:`gcReWriteLog` 会把仍被 LSM 引用的**旧版本**批量重写进**当前最新的 memtable**(经 `BatchSet` → writeCh),而更新的版本可能还在 immutable 或 L0。此时"memtable 里有这个 key"不再意味着它是最新的——早返回会读到旧数据。而且这个 bug **只需要 memtable 轮转就能触发,不需要落盘**,非常容易踩。

最终方案:**早返回加 `BitTxn` 门控**——只有普通事务条目(带 `BitTxn`)可提前返回;GC 重写的条目(不带 `BitTxn`)走全层取最大兜底。精确匹配(`== startTs`)不设门控,因为那时返回的必然是正确的新指针副本。

**验证**:新增回归测试 `TestVlogGCRewriteInversion`,构造"旧版本被 GC 重写 + 轮转"场景;**去掉门控,测试失败**——这才证明测试真的能抓这个 bug。

**追问**:这给你什么通用教训?→ **不变量是有所有者(owner)的**。引理 ② 成立的前提是"没有别的东西往 memtable 注入旧版本",这个前提在引入 vlog GC 时被打破了,但早返回的代码位置离 GC 很远,不会自然联想到。做优化时要把依赖的不变量显式写下来,并给每条引理找"谁会破坏它"。

---

### Q59. 项目里最有代表性的 bug 是哪个?

**考察点**:故事题,考察深度和表达。

**参考答案**:
**块缓存幽灵 key:约 1.4% 的 key 静默丢失**。这个 bug 最好的地方在于它是"能通过大部分测试、只在特定访问模式下偶发丢数据"的类型。

现象:全量写 10 万条、flush 后全量读回,约 1.4% 的 key 读不到。比例稳定但不是固定 key,说明与访问顺序有关。

根因在 W-TinyLFU 的 **SLRU 晋升**逻辑:stageOne(probation)命中要晋升到 stageTwo(protected),当 stageTwo 满时,实现是**交换两个 `list.Element` 的 storeItem 内容**(把队尾的败者换成当前元素,避免重新分配)。但 `s.data` 这个"key → element"的映射**没有跟着交换更新**,于是:
- element 的**位置**对(在哪个 list、哪个位置),
- 但它**承载的 key**变了,
- `s.data[hash]` 还指向交换前的 element → `get` 返回的是**另一个 key 的 value**。

再叠加一个致命细节:`get` 在 stage 操作之后**没有重新解析** `s.data[hash]`,直接就返回了。

修复两处:① 晋升交换时同步重写两个 map 索引;② `get` 在完成 SLRU 操作后**必须重新 `s.data[hash]` 解析**再返回(注释特意保留了这条,因为它是"看起来多余其实必需"的)。

**追问**:怎么抓到的?→ 我写了 `TestFlushReadIntegrity`(10 万 key,flush 后全量读回,**misses 必须为 0**)。这个测试是"全量校验"而不是"抽样校验"——抽样测试几乎不可能稳定命中 1.4% 的概率。**教训:缓存/索引这类结构的正确性测试必须做全量比对,不能抽样。**

---

### Q60. 除了幽灵 key,还有哪些有代表性的 bug?

**考察点**:广度,证明不是只修了一个。

**参考答案**:
按类别列(每个都能展开,文档在 `docs/improvements-detail.md`):
- **并发/生命周期**:`Table.DecrRef` 用普通读 `t.ref` 判零(并行子压缩并发 DecrRef 会读到竞争值)→ 必须用 `atomic.AddInt32` 的**返回值**判零;`getTxnDoneIndexFromCh` 协程启动后没人等待 → 关闭时泄漏;`discardStats` 协程泄漏;`GetTables`/`addSize` 无锁访问层结构;`vlog.maxFid` 原子与非原子混用。
- **逻辑/边界**:`Rotate` 的条件写反;`compactBuildTables` 用目标层号而非源层号(见 Q56);`gcReWriteLog` 查不到 LSM 条目就整个中止(应该当死数据跳过);`MergingIterator` 重扫(堆耗尽后原始迭代器列表没保存)。
- **崩溃/持久化**:空 WAL 无法重开(Q19);极小阈值下 finTxn 被写进 vlog(Q92);`sendDiscardStats` 解析两次 `ValPtr`(Q83);vlog mmap 扩容竞态 SIGSEGV(Q82)。
- **并发正确性(水位)**:`AtomicMark` 两个死锁(Q45/Q46);`LimitMark` 的 `notifyAndRemove(index)` 传错键导致 channel 二次 close(Q61)。

**追问**:这些 bug 里有几个是"-race 能抓到的"?→ 少数(`Table.DecrRef` 那种数据竞争能抓到)。**大部分抓不到**:死锁 `-race` 不报,静默丢数据 `-race` 不报,死代码路径 `-race` 不报。所以 `-race` 是必要不充分条件——**它证明"没发现数据竞争",不证明"逻辑正确"**。真正的保障是定向的功能测试 + 全量校验测试。

---

### Q61. `LimitMark` 的 waiter 二次 close 竞态是什么?

**考察点**:细节严谨性。

**参考答案**:
`processOne` 遍历等待者 map 时,稀疏分支写了 `notifyAndRemove(index, waiters)`——用的是**外层刚处理的 index**,而不是循环里取到的 map 键 `idx`。后果:
- `indexWaiters[idx]` 这个条目**没被删除**,但它的 channel 已经被 close;
- 下一轮再遍历到同一个 channel → **二次 close** → `panic: close of closed channel`。

修复:改成 `notifyAndRemove(idx, waiters)`。回归测试 `utils/limiterMark_test.go` 的 `TestLimitMarkWaiterNotifyRace`,注释里写的"期望 panic"就是修复前的行为。

**追问**:这类 bug 的特征?→ **"在循环体里用错了外层变量"**——一个变量遮蔽/取错的低级错误,但只在稀疏分支 + 特定 index 分布下触发,所以测试要构造出"同一 channel 被遍历两次"的分布。又是"必须定向构造"的例子。

---

### Q62. 零分配扫描是怎么做到的?从 20 万次分配降到 832 次

**考察点**:简历"扫描分配 -99.6%"的来源。

**参考答案**:
优化前:迭代 10 万条,每条 `Item()` 都做一次拷贝(key/value `SafeCopy`)→ 约 20 万次分配(注释实测 `ReadIterate` **200,000 allocs**)。

做了四件事:
1. **memtable 侧**:跳表迭代器的 `Key()`/`Value()` 直接返回 **arena 视图**(arena 只追加、不迁移,返回切片在表存活期内永久有效),不需要拷贝;
2. **SST 侧**:`TableIterator` 自带一个 **64KB `ChunkedArena`**,块内 key 从 chunk 里分配(块一经分配不移动),`Rewind` 时重置 arena;块内二分只解码不拷贝,**命中才** `setIndex` 拷贝;
3. **归并层**:`MergingIterator.Item` 直接返回堆顶,不做 SafeCopy,依赖子迭代器 Item 稳定;
4. **大 value 惰性**:见 Q63。

结果:`ReadIterate` 200,000 → **832 allocs(-99.6%)**,`~35ms`;**`ReadIterateSST` 直接 0 allocs / 0 B**,~26ms。

**追问**:为什么不是 0 而是 832?→ 迭代器创建本身、每层的迭代器对象、chunk 分配、以及一些必须在边界拷贝的地方(如 `ErrBlockEOF` 回退)。SST 路径能到 0 是因为它纯粹走 chunked arena + 栈上状态,没有边界拷贝。

---

### Q63. 大 value 惰性读取的 API 为什么这么设计?

**考察点**:API 设计与用户契约。

**参考答案**:
背景:扫 500 条 1MB 的 value,旧实现每条 `Item()` 都拷贝 1MB → 数据搬运量 528MB,几乎全是浪费(用户可能根本不需要 value)。

新设计:
- `TxnIterator.Item()` 遇到 `IsValPtr` 的条目**只解析出 12 字节 `ValuePtr`**,把 `entry.Value` 置为 nil,返回 `interfaces.Item{Item: entry, VP: lazyVP, Vlog: lazyVlog}`;
- 用户在需要时才调 `item.Value()`:`Vlog.Read(&VP)` → mmap 零拷贝切片 → `SafeCopy` → `callBack()` 释放文件读锁;
- 结果:扫 500×1MB 的**搬运量从 528MB 降到 65KB**(实测 B/op 48、2 allocs、~110µs),`ReadIterateBigValueEager`(用户主动取值)才付 528MB。

**API 契约的关键点**(也是面试官会追问的):
1. `Item.Value` 字段对小 value 有值,对大 value 是 nil —— **用户必须走 `Value()` 方法**,这是易错点;
2. `Value()` 内部做 `SafeCopy`,所以**返回的切片可以在迭代器关闭后继续使用**;
3. `callBack()` 必须被调用(内部 `Value()` 已处理),否则 vlog 文件读锁不释放,GC 删不掉文件;
4. 这条路径让"扫描只关心 key"的场景**完全不触碰 value**,这是 kv 分离最大的收益之一。

**追问**:为什么不在 `Item()` 里就自动读出来?→ 那就回到了旧实现,把"按需"变成"总是",违背惰性初衷。API 用"字段可能是 nil"来强制用户显式声明意图,是**用一点易用性换一个数量级的搬运量**。

---

### Q64. W-TinyLFU 缓存完整讲一遍

**考察点**:简历里的招牌组件,必须能画出来讲。

**参考答案**:
结构(见 `utils/cache/`):
- **窗口 LRU(15%)**:新条目先进这里,`get` 做 MoveToFront;淘汰队尾作为**候选者**。
- **准入过滤(SLRU 的 victim)**:候选者要与 SLRU 的 victim(probation 队尾)做频率 PK。
- **SLRU(85%)**:分 probation(stageOne,占 SLRU 的 15%)和 protected(stageTwo,85%)。stageOne 命中晋升 stageTwo;stageTwo 满时与队尾交换内容(败者回 stageOne)。
- **CM-Sketch**:4 行、4-bit 计数器、饱和 15,每行独立 seed,`Estimate` 取 4 行最小值抑制碰撞;**每 2 个计数器打包进 1 字节**;计数器总数 = 2×每片条目数;衰减时 `Reset` 是**整体减半**(`cr[i] >> 1`)而不是清零——保留近期性、给新热点让路。
- **Doorkeeper 布隆**(fp=0.005):`Allow(hash)` 语义是"之前见过返回 true,否则插入并返回 false"。它在**命中与否都调用**,让冷门但反复访问的 key 攒"准入资格"。
- **准入规则**:窗口淘汰的候选者要同时满足 ① doorkeeper `Allow` 为 true(频率 ≥ 2,防一次性扫描污染)② `CM.Estimate(candidate) >= CM.Estimate(victim)`,才进 SLRU,否则丢弃。
- **衰减**:访问计数达到 `threshold = numEntries * 100` 时,`cmkt.Reset() + door.reset()`。

**追问**:为什么窗口是 15%?→ TinyLFU 的经验值。窗口小是为了让新条目快速经过"频率测试",窗口大则等于放弃准入控制、退化成普通 LRU。SLRU 的 probation 15% 是"给刚晋升失败者一个观察期"。

---

### Q65. 块缓存为什么分 16 个分片?路由为什么用乘法散列?

**考察点**:并发优化的细节。`docs/read-path-optimization.md` 第八节。

**参考答案**:
**为什么分片**:W-TinyLFU 的 `get` 即使命中也要改 LRU 晋升和频次统计,所以**必须拿写锁**。全局单锁在读并发高时退化为 futex 竞争。分片后每把锁的流量降到 1/16。实测多协程缓存基准 685 → **361 ns/op(-47%)**。

**路由为什么用乘法散列**:缓存 key 是 `fid<<32 | blockIdx`(`lsm/table.go`),它是**结构化数值**——低位是 blockIdx,高位是 fid。如果直接取低位(`hash % 16`),blockIdx 的低 4 位分布会严重倾斜(同一个表的块索引连续,低位模式固定),导致分片负载不均。用乘法散列:

```go
c.shards[(hash*0x9E3779B97F4A7C15) >> (64-cacheShardBits)]
```

乘以黄金比例常数后取**高位**,低位的信息被"混进"高位,分布均匀且不需要取模。

**追问**:这里有个诊断故事?→ 有,而且是**误诊**。分片之前,SST 点读 profile 里 `runtime.futex` 占 12%,我最初判断是"块缓存全局写锁竞争"。但 `pprof -peek` 看调用来源发现:**97% 的 futex 来自 `runtime.notesleep/notewakeup`(GC 工作协程协调),真正的 mutex 竞争(`semasleep`)只占 2.7%**。12% 其实是**分配 churn 带来的 GC 开销**(825 B/op × 200 万次操作 ≈ 1.6GB 分配)。这个教训很重要:**不要看到 futex 就判定锁竞争,要看 `-peek` 的来源**。

---

### Q66. CM-Sketch 的计数器为什么是 4 bit?为什么衰减是减半而不是清零?

**考察点**:概率数据结构的参数直觉。

**参考答案**:
- **4 bit / 饱和 15**:频率估计不需要大范围,4 bit 够用且**省内存**——两个计数器打包进 1 字节(4.5 倍于 64-bit int 的密度)。饱和在 15 意味着"足够热",更大的精度对缓存准入决策没有增量价值。depth=4 行、`Estimate` 取最小值的经典设计,抑制哈希碰撞导致的高估。
- **衰减减半而不是清零**:TinyLFU 要解决的是"频率的时间局部性"——一次性扫描大量 key 会在 sketch 里留下大量 1 次计数,如果不衰减,新热点永远竞争不过历史扫过的 key(实际上 TinyLFU 的经典问题就是"扫描攻击")。**减半**保留**相对**频率信息(近期热点仍比近期冷门高),而**清零**会把所有历史信息一起丢掉,导致每次衰减后缓存决策变成随机。这和滑动窗口的设计意图一致:不是"忘记一切",而是"让老信息权重下降"。

**追问**:衰减周期为什么绑定"访问次数 = numEntries × 100"?→ 让它与缓存容量成比例:缓存越大,需要越多访问才认为"频率分布已经老化"。如果用固定次数,小缓存会衰减太慢、大缓存衰减太快。

---

### Q67. 8 项写路径分配优化分别是什么?

**考察点**:简历"8 项分配优化"必被追问到具体项。

**参考答案**:
(见 `docs/write-path-optimization.md`,配合 `TestXxx -benchmem` 数字)
1. `handleWriteCh` **内联写**:写令牌空闲时本协程直接写,忙才 `go`,省掉每批一个协程;
2. **事务池化**:`NewTransaction` 曾占 pprof 34.6%,池化复用结构体 + 两个 map;
3. **finTxn entry + ts 缓冲复用**:`finEntry`/`finTsBuf` 作为私有 scratch,提交完不释放;
4. **`conflictKeys` 快照化**:提交时拷贝成 `[]uint64` 存进 `commitedTxns`,使事务的 map 可安全复用;
5. **vlog/WAL 单次 CRC**:用 `crc32.Checksum` 就地算,替代 `crc32.New` + `io.MultiWriter` 的接口逃逸;
6. **`[]*ValuePtr` → `[]ValuePtr`**:切片元素从指针改成值,减少一层间接和分配;
7. **请求级 ValuePtr 编码缓冲**:`req.EncodeValPtr` 复用请求自带的 12B scratch,`lsm.Put` 同步消费后即可覆写;
8. **`KeyTsBufPool` 池化 `*[]byte`**:提交路径 `KeyWithTsPooled` 借、`Discard` 归还。

结果:`TrainKVTxnSet` 25 → **14 allocs/op(-44%)**,`TxnSetParallel` 18 → **8(-56%)**,`BatchSet10` 84 → **64(-24%)**,`TxnSetBigValue` 14 → **8(-43%)**;总分配 110,601 → 24,026(**-78%**)。

**追问**:哪一项收益最大?→ 事务池化(第 2 项),因为它是"每事务 3 次分配"的量级问题,其他都是常数因子级别的削减。

---

### Q68. 项目里踩过什么 `sync.Pool` 的坑?

**考察点**:细节控。

**参考答案**:
**把 `[]byte` 直接 `Put` 进 `sync.Pool` 会每次分配**。原因:Go 的 `sync.Pool.Put(x any)` 接收 `interface{}`,放一个 slice 进去要把 slice header **装箱**成一个接口值 → 每次 Put 都产生一次堆分配。这完全违背了池化的目的。

发现方式很直接:`Discard` 里一行 `KeyTsBufPool.Put(...)` 在 pprof 里贡献了 **43,691 次分配,每 op 约 50 次**,占了写路径分配的很大一块。

修复:池里存 **`*[]byte`(指针)**——指针装进 `interface{}` 不需要额外分配(Go 对小整数/指针的接口装箱可能复用,但关键是 slice 一定不)。取出来再 `*buf = buf[:0]` 复用底层数组。这就是 `model.KeyTsBufPool` 的设计。

**追问**:还有别的 pprof 陷阱吗?→ 有,**`pprof` 默认 512KB 采样率对大量小对象几乎失明**:小对象分配的调用栈采样很少,容易漏掉热点。定位上面这个坑时,我是靠 `-benchmem` 的 allocs/op 逐项对比 + 逐行注释才锁定到那一行。教训:**alloc 类问题优先看 `-benchmem` 和 `-m` 逃逸分析,不要只信 pprof 的采样**。

---

### Q69. 为什么用 `allocs/op` 评判写性能,而不是只看 `ns/op`?

**考察点**:性能分析方法论。

**参考答案**:
三个理由:
1. **`ns/op` 在高并发写场景噪声极大**:fsync 抖动可以从 2.6ms 到 38ms(注释实测),`ns/op` 一次跑下来差好几倍,根本分不出优化是否有效。而 `allocs/op` 是**确定性的**——同样的代码路径分配次数几乎不变,是信噪比极高的指标。
2. **分配直接决定 GC 压力**:写路径在热路径上,每 op 多一次分配,在高吞吐下就是每秒几百万次额外分配 → GC 频率上升 → STW 和辅助标记占用 CPU → 反过来影响延迟。所以分配次数是**延迟稳定性的先行指标**。
3. **可归因**:每个分配都有明确来源,能一项项列出来(这也是"8 项优化"能成立的原因);而 ns 抖动很难归因到具体代码。

实践方式:`go test -bench=. -benchmem` 看 allocs/op 和 B/op 做主指标,`ns/op` 做参考;优化前后用 `-count=3` 取中位数。

**追问**:那什么场景必须看 ns?→ 关注尾延迟和吞吐上限时。比如 `SyncWrites=true` 的 group commit 效果(`SyncGroupCommitWait` 该设多少)只能靠 ns/延迟分布判断,因为它的收益就是"减少 fsync 次数",与分配无关。

---

### Q70. 你发现自己写的基准测试有问题?怎么修?

**考察点**:测量诚实性,很能体现工程素养。

**参考答案**:
`bench_trainkv_test.go` 的 `Benchmark_TrainKV_GetValue` 有三个硬伤:
1. **`init()` 清空目录 A 却打开目录 B**(`perfDataDir`),DB 状态依赖历史运行,**不可复现**;
2. **`GetValue` 的 setup 在每次 benchmark 调用(含校准迭代)里重写 50 万条**,计时区外写入触发的后台 flush/compaction 分配仍会计入 `B/op`;
3. **读范围超出写入范围**(只写了 0..499999,却读到 `b.N`),约 1/3 的读是 miss。

修复:每个 benchmark 独立清目录、独立打开;setup 移到 `ResetTimer` 之前并等待落盘稳定;读用预生成的随机序并取模限制在写入范围内。

量化结果:`TrainKV_GetValue` 从 13989 ns/op / 4383 B/op / **30 allocs** → **4590 ns/op / 825 B/op / 7 allocs**。注意这个降幅是**测量修复**而非读路径优化——修复后它才测的是真实负载(20 万条全落盘 L0 的随机点读)。`PutValue` 的量级基本没变。

**追问**:为什么一个错误的基准可能让你做出错误的优化决策?→ 修之前 profile 把时间花在 miss 和后台任务上,会诱导你去优化"不存在的读路径"。**基准错了,后面所有 profile 和优化都是错的**。所以我现在的纪律是:优化之前先问"这个基准量的是什么"。

---

### Q71. 讲了这么多优化,有没有失败的优化?

**考察点**:诚实度和科学方法。

**参考答案**:
有,而且是明确记录下来的负收益实验:**软件预取**。跳表下潜是**串行依赖链**(必须比完当前节点才知道下一跳),我尝试用 dummy load 做软件预取来掩盖 cache miss:

| 方案 | 结果 | 结论 |
|---|---|---|
| 预取"同层前进候选" | 1790 → **2040 ns/op(-13%)** | 下潜路径上预取目标大多不是实际下一跳,纯添乱 |
| 预取"降层候选" | 1790 → **1899 ns/op(-6%)** | 同样负收益 |

原因:Go 没有显式预取指令,只能靠 dummy load,而**指令开销 + 低命中率**在这个硬件上不划算。全部还原了。

**追问**:那读路径的残余成本到底是什么?→ **结构性的**:一次 Get 约 16–20 跳,每跳**串行**等 2 行冷缓存(节点字段 + key 字节,在 57MB 的 arena 里相距约 560B),约 80–100 ns/跳;固定开销 200–300ns。profile 显示 60%+ 在 `findNear`。这**不是代码缺陷,是 skiplist memtable 随机点读的物理成本**(Badger 同量级)。真正的结构性提速要换索引结构(比如 B+ 树 memtable 或用更紧凑的节点布局减少 cache line 数),那是另一个量级的工程。

**追问**:负收益实验要不要写进文档?→ **要**。它证明我在做实验而不是碰运气,也给后来者省时间("别试预取,已经试过").

---

### Q72. 测试体系是怎么建的?

**考察点**:工程规范。

**参考答案**:
分四层:
1. **端到端功能测试**:`db_test.go` 覆盖 reopen、锁冲突(`ErrLockDB`)、基本 CRUD、批量、迭代器、MaxVersion、并发操作;`db_test.go` 的 `TestDBReopenAfterManyRotations`(MemTableSize=8MB 写 20 万条跨多次轮转后重开全量读回)。
2. **崩溃恢复测试**(`crash_test.go`):用**子进程**模拟——同一测试二进制以 `TRAINKV_CRASH_CHILD=1` 启动,写入 50 条后**不调 `db.Close` 直接 `os.Exit(0)`**,模拟掉电/`kill -9`;父进程再 Open 并逐 key 校验。两个子场景:`clean-crash` 和 `torn-wal-tail`(用 `0xDE` 覆盖制造撕裂记录,见 Q14)。
3. **并发与正确性定向测试**:`TestFlushReadIntegrity`(10 万 key 全量读回,**misses 必须为 0**——抓幽灵 key 的测试)、`TestDBAutoCompaction`(压缩期间**并发读**,校验更新取新值/删除 not found/L0 表数下降)、`TesValueLogGCFull`(更新+删除造 discard 统计 → RunOnce 生成 → 并发读期间 GC → 校验存活值/旧文件被删 → 重启后再校验)、`TestVlogGCRewriteInversion`、`TestAtomicMarkWakeAfterAbsorb`、`TestLimitMarkWaiterNotifyRace` 等。
4. **纪律**:全量 `go test ./... -race -count=1`;基准 `-benchmem -benchtime=2s -count=3`。

**追问**:为什么崩溃测试要用子进程而不是"模拟"?→ 因为 `os.Exit(0)` 会**跳过所有 defer 和 Close**,这才是真实的掉电语义。在同一个进程里"手动把文件关掉"会顺带做很多清理,测不出真实崩溃窗口。这个技巧的代价是需要把子进程逻辑写进测试自身并靠环境变量分流。

---

### Q73. `-race` 能保证什么?不能保证什么?

**考察点**:对工具的边界有清醒认识。

**参考答案**:
**能**:检测到**实际发生过的**数据竞争(两个 goroutine 无同步地访问同一内存且至少一个写),基于 happens-before 的 happens-before 分析。它的价值在于"发现的是真实执行过的竞态",误报率低。

**不能**:
- **抓不到没被触发的竞态**:必须同一时刻两个协程抢同一地址 —— 所以需要 `-count` 重复跑、需要压测;
- **抓不到死锁**:goroutine 永久等待不是数据竞争,`-race` 完全不管(本项目两次 `AtomicMark` 死锁都没被 `-race` 报出来);
- **抓不到逻辑错误**:静默丢数据(幽灵 key)、finTxn 写错位置、压缩分支永远不走,这些与竞争无关;
- **不保证不丢数据**:它只保证"没有未同步的访问",不保证业务语义。

**所以我把它定位为"必要不充分"**:它是我测试纪律的一部分,但正确性主要靠**定向功能测试 + 全量校验 + 崩溃注入**三道闸。`-race` 抓不到的那些问题,需要"我自己想到这个场景"才能测到——这也是为什么我会主动列出 `TestVlogGCRewriteInversion` 这种"为了一个不变量专门写的"测试。

---

### Q74. 你的测试里哪几个是"找到真 bug"的,哪几个是"防回归"的?

**考察点**:测试的成熟度。

**参考答案**:
**找到真 bug 的**(先有失败,后有修复):
- `TestFlushReadIntegrity` → 幽灵 key(全量校验才能命中 1.4%);
- `TestOpenWithEmptyWAL` → 空 WAL 无法重开;
- `TestDBEmptyTxnCommitReopen` → 空事务产生孤儿 FIN;
- `TestVlogGCRewriteInversion` → `<=` 早返回漏 `BitTxn` 门控;
- `TestAtomicMarkWakeAfterAbsorb` / `TestAtomicMarkConcurrentDone` → 两个水位死锁;
- `TestLimitMarkWaiterNotifyRace` → 二次 close;
- `TestCompactionL0ToL0` → 压缩死代码(测试进不去分支);
- `TestReopenTinyValueThreshold` → finTxn 被判进 vlog;
- `TestVlogRemapConcurrentRead` → mmap 扩容竞态;
- `TestDiscardStatsSurvivesReopen` → discardStats 双解析。

**防回归的**:`TestDBAutoCompaction`、`TestCompactionL0ToLmax`、`TestLastLevelCompaction`、`TestDBReopenAfterManyRotations`、`TestDBConcurrentOperations`、`TestReOpen` 等——它们覆盖主流程,防止后续改动破坏既有行为。

**追问**:这两类测试的写法差别?→ **找 bug 的测试必须有"能失败的证据"**:我先让修复前的代码跑,确认它**稳定失败**;如果是偶发,就调到能稳定复现(比如 `TestLimMarkWaiterNotifyRace` 的注释直接写了"期望 panic")。不能失败的测试不是回归测试,是装饰。

---

### Q75. 性能报告里的数字,哪些是你自己测的,哪些是你需要谨慎引用的?

**考察点**:数字诚实性。

**参考答案**:
**我自己的机器上、可复现的**(i5-7300HQ / WSL2 / GOMAXPROCS=4):读路径 2367→1729 ns/op、1224→801 ns/op;写路径 25→14 / 18→8 allocs/op;扫描 200,000→832 / 0 allocs;大 value 528MB→65KB;缓存分片 685→361 ns/op。这些都是 `-benchmem` 出的,有 `docs/` 记录。

**需要谨慎引用的**:
- **YCSB 的跨引擎对比数字**——它们依赖硬件、内核、对应引擎版本(这里是 badger 4.9.6 / pebble 1.1.5)、参数配置。我能断言的是"**在这套固定环境和参数下**"的相对值(trainkv/badger 在 workload b/c 上约 1.97x / 2.06x,大 value 场景 trainkv/pebble 约 2–3x),但不能外推成"TrainKV 比 Badger 快 2 倍"。
- **`ns/op` 有抖动的项**(同步写路径)——fsync 抖动 2.6ms~38ms,所以那些数字要报量级而不是精确值。
- **`benchmk_1` 里那些很旧的跨引擎数字**(比如 TrainKV PutValue 30598 ns)——不同时期、不同配置,不能和现在的数字混着用。

**追问**:面试时怎么报数字?→ **报"在什么条件下测得什么"**,而不是报一个孤零零的数。比如"在 value=1KB、16 线程、记录数 10 万、关闭 sync 的配置下,workload C 纯读吞吐 trainkv 约 10.3 万 ops/s,badger 约 4.8 万"。有条件的数字才可信,没条件的数字一问就穿。

---

### Q76. `sync.WaitGroup` / `Throttle` / `Closer` 这些并发原语你封装了什么?为什么?

**考察点**:基础库的工程化使用。

**参考答案**:
- **`utils.Closer`**:`{waiting sync.WaitGroup; CloseSignal chan struct{}}`。`CloseAndWait()` 先 `close(CloseSignal)` 唤醒监听它的协程、再 `waiting.Wait()`。用途:每个后台组一个 Closer(`memtable`、`compactors`、`writes`、`valueGC`),`Close` 时按组**顺序**停:先停生产者(compactor、GC、事务管理器、discardStats)→ 再排空写通道 → 最后刷盘。顺序错了会出现"消费者先死、生产者还在写" → 丢数据或死锁。
- **`utils/throttle.go`**:动态并发限流。`Do()` 抢令牌(容量 max 的 channel)成功则 `wg.Add(1)`,失败则读 `errCh` 拿错误;`Done(err)` 投错误、还令牌、`wg.Done()`;`Finish()` 用 `sync.Once` 包 `wg.Wait()` 后 close 两个 channel,收集最后一个非 nil 错误。压缩里用 `NewThrottle(8+len(splits))` 限制并行 builder 数。
- **`common.Check` + `AssertTrue/AssertTruef`**:区分"可恢复错误"和"内部不变量被破坏"。不变量破坏用 `log.Fatalf` 直接终止,因为继续跑只会产生错误数据——LSM 引擎在"元数据不一致"状态下继续运行比崩溃更危险。

**追问**:有没有一个"必须顺序关闭"的反例?→ 有,而且是已知的坑:`db.go` 注释记录,**如果 `handleWriteCh` 因为写失败而退出,`writeCh` 就失去消费者,所有后续提交永久阻塞**。所以写失败只记录错误、绝不退出攒批协程。这是"消费者不能先死"的直接体现。

---

# 第五层:系统设计与复盘(开放题,Q77–Q100)

### Q77. kv 分离的原理是什么?阈值为什么定 1MB?

**考察点**:核心架构设计。

**参考答案**:
**原理**:LSM 的压缩会反复移动数据,而大 value 移动的代价极高(写放大 = 层数 × value 大小)。把大 value 从 LSM 里剥出去、追加写进单独的 value log(vlog),LSM 里只留 **12 字节 `ValuePtr`(fid/offset/len)**,那么:**压缩只搬指针,不搬 value**。代价是读大 value 多一次 vlog 寻址(为局部性通常用 mmap 零拷贝缓解),以及需要 vlog GC 回收失效 value。

**阈值为什么是 1MB**:这是"LSM 写放大"与"vlog 随机读放大"的平衡点。阈值太小 → 大量中等 value 进 vlog,读路径多一次跳转、GC 压力大;阈值太大 → 大 value 留在 LSM,压缩搬大 value,写放大爆炸。经验上 1MB 左右是分界(而且很贴合"按 value 大小分而治之"的直觉)。**本项目历史上从 32B 改到 1MB**(commit 14aab29):32B 太小,几乎所有 value 都进 vlog,完全退化成"纯 vlog",LSM 失去意义。这个改动本身就是"阈值应该匹配实际 value 分布"的例证。

**追问**:还有其他方案吗?→ 有。① 阈值按数据分布自适应;② 大小 value 分不同区域(RocksDB 的 blob file 更接近这个);③ 只对"超大 value"(比如 > 4KB)分离,小 value 内联减少读跳转。TrainKV 目前是单阈值的简单策略。

---

### Q78. `ValuePtr` 为什么是 12 字节?它的编码有什么隐患?

**考察点**:对"紧凑编码"的批判性思考。

**参考答案**:
```go
type ValuePtr struct {
    Len    uint32   // 记录总长 = headerLen+KLen+VLen+4(crc)
    Offset uint32   // 记录在 vlog 文件内的起始偏移
    Fid    uint32   // vlog 文件 id
}
```
三个 uint32 共 **12 字节**。精简的原因:LSM 里每个大 value 条目都要存这个指针,而且**压缩时会反复搬运它**,所以字节数直接影响 LSM 大小和压缩成本。与 8 字节指针方案相比,32 位的限制是:单 vlog 文件 ≤ 4GB(由 `ValueLogFileSize` 和文件数满足)、单条记录 ≤ 4GB。这些约束在默认配置(`ValueLogFileSize ≈ 1GB`)下完全够用。

**编码隐患**(我认可这是可以改进的地方):
- 编解码用 `unsafe` 直接按**内存布局整体拷贝**:

```go
func (p ValuePtr) Encode() []byte {
    buf := make([]byte, vptrSize)
    *(*ValuePtr)(unsafe.Pointer(&buf[0])) = p
    return buf
}
```
  这意味着编码结果依赖**本机字节序**——它**不做跨端移植**。同一份数据在大端机器上读小端机器写的库会得到错误的值。对单机嵌入式引擎可接受,但要"跨平台共享数据文件"就必须改成显式 `binary.BigEndian`。
- `Decode` 里有长度检查 `if uintptr(len(b)) < vptrSize { panic }`,但 panic 在生产路径上是重手段——正常情况不会触发,属于"防御性断言"。
- 如果未来要支持列族或更细的 GC 粒度,12 字节可能不够,需要重新设计(比如把 fid 扩到 40 位、或引入 generation)。

**追问**:面试官可能问"小端大端" —— 我要能立刻说清楚:**当前实现是主机序,不可移植;改成显式序是 3 行代码的事,但会让编解码多几个字节的指令**。这是"性能 vs 可移植性"的显式取舍。

---

### Q79. "vlog 单次 CRC" 优化改了什么?为什么原来会慢?

**考察点**:简历里有"vlog 单次 CRC",要能讲清。

**参考答案**:
原来的写法基于 `io.MultiWriter(out, hash)`,每段 header/key/value 都经 `hash.Hash` 接口 `Write`,最后连 crc 字节也经同一个 writer 写进去。问题有两层:
1. **接口逃逸**:`hash.Hash` 是接口,`Write` 调用会把 `headerBuf` 等 buffer 逃逸到堆(注释明确记录了这一点),每次写都在分配;
2. **双重路径 + 双拷贝**:先写 `bytes.Buffer` 再 `AppendBuffer` 拷进 mmap,数据被走两遍;而且 crc 字节"既进缓冲又再次喂给 hash",是冗余的哈希输入。

改成 `EncodeEntryAt`:直接把 header/key/value `copy` 进 mmap 的 `vlog.f.Buf[offset:]`,再对**已写入的那段**就地算一次 `crc32.Checksum(dst[:n], CastagnoliTable)`,结果追加在末尾。一次拷贝、一次哈希、零接口调用。

量化:大 value 的 B/op 从 **12,820 降到 115**;WAL 侧同模式(commit `2f7ca8d` "删除 wal 双拷贝")。

**追问**:为什么 CRC 覆盖 header+key+value 而不是只覆盖 value?→ 因为恢复时要校验**整条记录的完整性**:key 被损坏和 value 被损坏一样致命(可能把 value 挂到错误的 key 上)。覆盖全记录是标准做法。

---

### Q80. vlog 的 mmap 扩容有什么竞态?怎么发现和修的?

**考察点**:最有含金量的 bug 之一(commit 4675bbc)。

**参考答案**:
**现象/根因**:Linux 上 `MmapFile.Truncate` 走 `mmap.Mremap`,底层 `SYS_MREMAP` 带 `MREMAP_MAYMOVE=0x1`——**内核可以把整个映射搬到新的虚拟地址**,同时 `m.Buf` 被换成新地址。而读者是在 `VLogFile.Lock.RLock` 下通过 `Read → vlog.f.Buf` 拿切片并 `SafeCopy` 的;**扩容如果不加写锁**,就会读到已经被 unmap 的旧地址 → **SIGSEGV**。大 value 并发读写时必现,`-race` 也能命中。

**修复**:把扩容包在写锁里,与读者的读锁互斥:
```go
// Truncate 内部 mremap(MREMAP_MAYMOVE) 可能把映射搬到新地址;
// 读者持有 VLogFile.Lock.RLock 期间直接引用旧映射做 SafeCopy, 扩容必须用写锁互斥,
// 否则会读到已解除映射的地址 (SIGSEGV, 大 value 并发读写必现);
vlog.Lock.Lock()
err := vlog.f.Truncate(growTo)
vlog.Lock.Unlock()
```
`DoneWriting` 里"取消映射 → 重映射"同样用写锁保护。

**回归测试** `TestVlogRemapConcurrentRead`:64KB value、1MB 文件,关库截断后重开(让映射紧贴数据),8 个读协程 + 56 次写触发 mremap。

**追问**:为什么扩大容量时一次扩到 `2*ValueLogFileSize` 而不是刚好到记录末尾?→ 注释解释:避免"关库截断后重开、映射紧贴数据末尾"时**每条追加都触发 Truncate**(整文件 msync + ftruncate + mremap)。一次扩大是拿少量虚拟地址空间换掉高频扩容。

**追问 2**:这个 bug 的通用教训?→ **mmap 不是内存,是"映射"**。任何可能改变映射的操作(remap/truncate/munmap)都与"持有切片"互斥。用 mmap 做零拷贝读时必须显式管理这个生命周期,Go 的 GC 帮不了你。

---

### Q81. 一个大 value 从淘汰到回收,GC 全链路走一遍

**考察点**:vlog GC 的端到端理解。

**参考答案**:
1. **产生失效信息**:压缩时丢弃任何带 `ValuePointer` 的条目,就把 `discardStats[fid] += vp.Len`(`compact.go` 的 `updateDiscardStats`),经 `DiscardStatsCh`(容量 16)投递给 vlog 的 `handleDiscardStats`;
2. **持久化统计**:`handleDiscardStats` 合并进 `FileMap[fid]`,每累计超过 `discardStatsFlushThreshold=100` 次更新就把整个 FileMap `json.Marshal`,以一条**普通 Entry** 写回 LSM:key 是 `VlogFileDiscardStatsKey`、ts 用哨兵 `common.MaxTs`。这样**重启不丢统计**;
3. **触发 GC**:`db.RunValueLogGC(discardRatio)`(ratio 必须在 (0,1))→ `runGC` 用 `GarbageCh`(容量 1)当**门栓**,抢不到返回 `ErrRejected`(防并发 GC);`pickVlogFile` 排除正在写的 `maxFid`、文件数 ≤1 返回 nil,在所有文件里选 `FileMap` 最大且 `discard >= discardRatio * 文件大小` 的,否则 `ErrNoRewrite`;
4. **重写**:`gcReWriteLog` 逐条读旧文件,对每条调 `lsm.Get(key)`:
   - LSM 里没有(`ErrKeyNotFound`)→ 版本已被压缩清掉,丢弃;
   - `IsDiscardEntry` 为真(版本不等 / LSM 已是墓碑或过期 / LSM 该 key 已不再是指针)→ 丢弃;
   - 解出 LSM 里的指针,`fid/offset` 与当前记录**完全相等** → 存活,`SafeCopy` 后攒批;
   - LSM 指向**更新**的副本(fid/offset 更大)→ 丢弃;指向**更旧**的位置 → 也是死版本,丢弃;
   - 存活条目按 `MaxBatchCount/MaxBatchSize` 分批(批 1024,`ErrTxnTooBig` 减半),经 `db.BatchSet` 重写到当前 vlog 并更新 LSM;
5. **删文件**:`gcReWriteLog` 末尾持 `filesLock` 检查 `getIteratorCount()`:为 0 直接 `deleteVlogFile`,否则挂到 `FilesToDel`,等最后一个迭代器 `Close` 把计数降到 0 时统一删。`deleteVlogFile` 先 `VLogFile.Lock.Lock()` 等所有读者释放,再 Close(unmap)后 `os.Remove`;
6. **重启**:`Open` 末尾 `sendDiscardStats` 从 LSM 读回那个 JSON key,反序列化后投回 `FlushCh`,统计恢复。

**追问**:为什么 GC 不直接"读文件里所有活 value 重写、然后删文件"?→ 那需要能判断"活",而"活"的定义依赖 **LSM 当前指向哪个位置**,所以必须逐条反查 LSM。这也是为什么 GC 必须与压缩协同(压缩产生 discardStats,GC 消费它来决定动哪个文件)。**kv 分离和分层压缩是耦合的**,不是两个独立模块。

---

### Q82. `sendDiscardStats` 解析两次 `ValPtr` 的 bug 是什么?

**考察点**:小而典型的"语义误用" bug(commit 5b3ada4)。

**参考答案**:
旧代码在从 LSM 读回 discard 统计条目后,又检查了一次 `model.IsValPtr(entry)` 并 `Decode` 前 12 字节当 `ValuePtr` 去 `vlog.Read`。

**根因**:`db.get` 返回的 entry **已经解引用完毕**——如果原值在 vlog 里,`vlog.Read` 已经把 JSON 正文读出来了。但 `Meta & BitValuePointer` 这个位**被保留着**(它表示"这条数据原本存于 vlog",不是"当前 value 还是指针")。于是旧代码把 **JSON 正文的前 12 字节当成 ValuePtr**,拿一个不存在的 fid 去 `getVlogFileLocked` → `not found` → `Open` 里 `panic("Failed to populate discard stats")`,**库无法重开**。

修复:删掉二次解析,直接用 `entry.Value`。

**配套修复**(很重要):把 ts 哨兵从 `math.MaxUint64` 改成语义化的 `common.MaxTs`,并让 `memTable.put` **排除 `MaxTs` 不参与 maxVersion 统计**——否则重开时 `maxVersion` 被顶到溢出,`nextTxnTs` 归 0,整个 MVCC 时间线崩掉。回归测试 `TestDiscardStatsSurvivesReopen` 覆盖"value 内联 LSM"和"落 vlog"两种情况。

**追问**:为什么会有 `Meta` 位"描述历史而非现状"这种设计?→ 因为 `BitValuePointer` 原本的语义是"**这条 entry 的 value 字段在不带该位时才是真值**",它同时服务于写入路径(决定要不要替换成指针)和读取路径(决定要不要去 vlog 取)。`db.get` 解引用后返回的是一个"逻辑上完整"的 entry,但**物理标记位没有回滚**——这就是两个语义混用一个 bit 的代价。更干净的做法可能是解引用后清掉该位,但那会影响其他调用方。

---

### Q83. GC 怎么保证不与并发读冲突?

**考察点**:多道防线的一致性。

**参考答案**:
三道锁 + 一个计数 + 一个契约:
1. **`filesLock`(管理文件 map)**:所有按 fid 找文件的操作先 `RLock`,GC/删除文件时加写锁——防止"读到已从 map 移除的文件";
2. **`VLogFile.Lock`(单文件读写锁)**:读者 `RLock` 下拿 mmap 切片并 `SafeCopy`,`deleteVlogFile` 和扩容用 `Lock` 等所有读者退出;
3. **callBack 契约**:`vlog.Read` 返回的切片是 mmap 视图,调用方必须在使用完(或 `SafeCopy` 完)后调用返回的 `callBack`(就是 `RUnlock`)来释放文件读锁。这个契约不遵守 → GC 永远删不掉文件;
4. **迭代器引用计数**:长事务的迭代器可能跨越很长时间,所以 GC 重写完成后不立即删文件,而是检查 `getIteratorCount()`:非 0 就挂进 `FilesToDel`,等最后一个迭代器 `Close` 时统一删;
5. **`GarbageCh` 门栓**:一次只允许一个 GC 在跑,避免两个 GC 互相重写;

另外:读路径 `getVlogFileLocked` 对 `maxFid` 还有额外校验 `vp.Offset < writableFileOffset`——防止读到"指针已写但 value 还没写完"的位置。

**追问**:这么多道防线里哪一道最关键?→ **callBack 契约**。因为它是唯一一个"由调用方保证"的点,其他都是引擎内部可以保证的。它也是我在迭代器惰性读取那部分特别强调"用户必须调 `Value()` 而不是直接用字段"的原因——**API 把释放责任包在了 `Value()` 里**,就是为了不让用户漏掉 callBack。

---

### Q84. GC 判断一条记录"该丢还是该留"的依据是什么?

**考察点**:细节题,`IsDiscardEntry` 的逻辑。

**参考答案**:
`IsDiscardEntry(vlogEntry, lsmEntry)`(见 `model/ValuePtr.go`)为真则丢弃,条件任一成立:
1. **版本不等**——vlog 里的记录版本与 LSM 当前记录的版本不同,说明它是被覆盖的旧版本;
2. **LSM 已是删除或过期墓碑**——用户删了,value 自然无意义;
3. **LSM 该 key 已不再是指针**——说明这个 key 后来被写成了小 value(内联进 LSM),原来的 vlog 记录失效。

再配合 `vlog.go` 里对指针的比较:
- `vp.Fid == logFile.FID && vp.Offset == vlogEntry.Offset` → **存活**(LSM 精确指向这条记录);
- `vp.Fid > logFile.FID || vp.Offset > vlogEntry.Offset` → LSM 指向更新的副本,当前是旧的 → 丢;
- 反之(fid/offset 更小)→ 当前记录比 LSM 指向的还新?那不可能,按注释里的例子(vlog_1/vlog_9/vlog_10)它是死版本 → 丢。

`lsm.Get` 返回 `ErrKeyNotFound` 也是丢(版本已被压缩清掉)。

**追问**:为什么"三个条件"而不是"直接比指针"?→ 因为指针不只在"同一个 key 被覆盖"时失效,还会因为**删除**和**value 变小改内联**失效——这两种情况下旧指针根本没机会被比较,必须先看 LSM 条目的 Meta 语义。**判断"活"必须综合版本、Meta 位、指针位置三方面**。

---

### Q85. "单写者 + group commit"和"多写者并发"的取舍?

**考察点**:架构级权衡。

**参考答案**:
本项目的写路径是**单写者**:所有写经 `writeCh` 汇聚到一个消费者(`handleWriteCh`),再由容量 1 的 `blockChan` 保证任意时刻一个 `WriteRequest` 在跑。

**收益**:
- 跳表可以做到"写者唯一、读无锁",省掉大量同步开销;
- WAL/vlog 的 writeOffset 不需要 CAS,直接普通变量推进;
- group commit 天然实现:攒批、一次 fsync 摊到多个请求;
- 顺序可预测,L0 表的 MaxVersion 单调,读路径排序简单。

**代价**:
- **写吞吐上限被单核限制**,无法利用多核做并发写;
- 一个大请求(比如超大批量)会阻塞后面所有请求;
- WAL 和 vlog 写入无法并行。

**对比**:RocksDB 用多个 memtable + 写组并发(允许一定并发度),代价是复杂的同步和内存管理。对嵌入式/中低并发场景,单写者 + group commit 的**简单性和尾延迟可预测性**通常更值;对高并发写服务,多写者更合适。

**追问**:如果要做多写者,第一步改什么?→ 先把"跳表写"和"WAL 写"解耦:WAL 仍串行(保证日志顺序),跳表允许多写者(用更完整的 CAS 或分片跳表)。但那样就失去了"读无锁"的前提,需要重新设计读的同步——**收益和复杂度都要重新评估**,不是一个便宜改动。

---

### Q86. YCSB 的 A–F 分别是什么?你的测试选择了哪些?为什么?

**考察点**:基准测试的专业性。

**参考答案**:
| Workload | 读 | 写 | 特点 |
|---|---|---|---|
| A | 50% | 50% update | 典型混合 |
| B | 95% | 5% update | 读多写少 |
| C | 100% | 0% | 纯读 |
| D | 95% | 5% insert | **读最新**(read latest) |
| E | 95% scan | 5% insert | 短范围扫描 |
| F | 50% read | 50% read-modify-write | RMW |

本项目的对比报告(`docs/bench-report-...md`)跑的是 **a/b/c/e**:
- **b/c** 是"读多/纯读",用来验证读路径优化(块缓存、布隆、早返回)的效果;
- **a** 是混合,验证事务冲突检测与读写并发;
- **e** 是扫描+插入,验证迭代器和增量写;
- 没跑 **d/f** 是因为 `d` 的"read latest"语义需要额外的读最新 API 支持、`f` 的 RMW 在本项目里要用事务的 Get-Then-Set 模拟(会有冲突重试),两者都会让对比口径变复杂,属于"知道但没做"的部分。

另外设计了两组场景:**场景 A(value=1KB 全内联)** 和 **场景 B(value=2MB 触发 vlog)**,这是刻意覆盖"kv 分离开关"这个变量——因为它是本项目最核心的架构差异。

**追问**:workload A 里 update 怎么做?→ `ycsb_repro_test.go` 里 update 是**事务内先读再写(RMW)**,并且 `ErrConflict` 有重试上限 200。这保证测的是带冲突检测的事务写,而不是裸覆盖写。

---

### Q87. 和 Badger/Pebble 的对比结果怎么解释?

**考察点**:能否诚实且有洞察地解读数据。

**参考答案**:
(环境:WSL2 / i5-7300HQ 4 核 8 线程 / 7GB / Go 1.27;统一 `sync_writes=false`、vlog 阈值 1MB、badger 无压缩;测 ops/s。)

**场景 A(1KB value,全内联)**:
| 阶段 | trainkv | badger | pebble | trainkv/badger | trainkv/pebble |
|---|---|---|---|---|---|
| load | 33.2k | 24.4k | 28.6k | 1.32x | 1.21x |
| a(50/50) | 54.2k | 32.7k | 85.6k | 1.67x | **0.62x** |
| b(95/5) | 104.9k | 52.7k | 62.7k | 1.97x | 1.56x |
| c(纯读) | 102.7k | 47.9k | 110.6k | 2.06x | **0.90x** |
| e(scan/ins) | 3.3k | 1.4k | 5.3k | 2.45x | **0.62x** |

**场景 B(2MB value,触发 vlog)**:trainkv/pebble 在 load/a/c 分别是 **2.95x / 1.99x / 3.06x**;trainkv 与 badger 同档(0.66x–1.17x)。

**解读**:
- **对 badger 全面领先**(1.3–2.5x):都在同一架构族(kv 分离 + LSM + W-TinyLFU),差距主要来自本项目的读写路径优化(早返回、零分配扫描、分片缓存、事务池化)和参数差异。
- **对 pebble:读多/纯读占优,混合和扫描吃亏**。pebble 是 RocksDB 血统,写路径多线程、压缩更成熟,所以 50/50 混合和扫描吞吐更好;而本项目**单写者**在混合负载下是瓶颈。纯读上本项目略逊(0.90x)也说明块缓存/索引还可以再优化。
- **大 value 场景是 Clean Win**:pebble 把 2MB value 留在 LSM 里,压缩要反复搬 → load 3x、纯读 3x 的差距。这**正是 kv 分离设计要解决的问题**,数据支持了设计假设。

**追问**:有没有对你不利的数字?→ 有,而且我主动列了:workload a 0.62x、e 0.62x、c 0.90x。**报对自己不利的数字反而增加可信度**,而且它们指出的正是我明确的短板(单写者、压缩调度)。面试里如果只报 2.06x 而藏起 0.62x,一旦被追问就崩。

---

### Q88. 为什么大 value 场景下你的引擎对比 Pebble 优势最大?

**考察点**:能否把数字和设计连起来。

**参考答案**:
因为大 value 场景**放大的是 LSM 的写放大**,而这正是 kv 分离要解决的:
- Pebble 把 2MB value 放在 LSM 里 → 一次 flush、每次压缩都要搬 2MB;一个 key 经过 7 层压缩,L0→L6 的搬运量是 value 大小的数倍;
- TrainKV 把 value 放 vlog,**压缩只搬 12 字节指针** → 压缩的 IO 量相差 5 个数量级(2MB vs 12B);
- 读路径上,vlog 用 mmap + 大文件顺序布局,加上 `MadviseSequential` 的顺序 hint,Q(纯读)的局部性也不错。

所以场景 B 里 load 2.95x、纯读 3.06x 是这个设计的**预期收益**,不是调参调出来的。

**追问**:那 badger 也是 kv 分离,为什么你没拉开差距?→ 因为 badger 也是同一设计,差距回到了"实现质量 + 参数"层面;而且 badger 在这个场景下 load 还略快(1.17x),说明它的 vlog 写路径/线程模型有值得我学的地方。**"和同类设计打平、和异类设计拉开设计性差距"是一个合理的、可以解释的结果。**

---

### Q89. `SyncWrites` 开与关,延迟差多少?为什么?

**考察点**:持久性语义的代价。

**参考答案**:
(本机实测,`-benchmem`)
| 场景 | SyncWrites=false | SyncWrites=true |
|---|---|---|
| 单条写(128B) | ~3.2–3.4 µs/op | **~4.03 ms/op** |
| 4 并发写 | ~3.8 µs/op | **~2.1 ms/op**(group commit 把 4 次 sync 摊成 1 次) |
| 1MB 大 value | ~3.7 ms/op | ~25 ms/op |

差 **三个数量级**。原因:关掉时写只进 memtable + WAL buffer,不退化成磁盘等待;开启时每个批次要 `fsync`(WAL + 可能 vlog),单次 fsync 约 4ms 量级。

group commit 的效果直观:4 个并发提交从"4×4ms"降到"约 1×4ms 摊到 4 个请求" → 2.1ms/op。

**追问**:默认值是什么?为什么?→ 默认 `SyncWrites=false`(`lsm/option.go`),即**默认不保证单条提交的持久性**。这是一个性能优先的默认值,适合嵌入式/缓存/可重建数据的场景;但**对需要持久化的场景必须显式开启,并在文档里说清楚**。顺便一提:`README.md` 里写的是 `SyncWrites (true)`,与代码默认值不一致——这是**必须修掉的文档错误**。(见 Q96)

**追问 2**:`SyncGroupCommitWait=1ms` 怎么体现?→ 4 并发时单批原本可能只 1 个请求,1ms 汇合窗口等来了其他 3 个,一起 fsync,所以从 4 次 sync 变 1 次。这个 1ms 是"额外延迟换 fsync 摊销"的直接体现。

---

### Q90. 你的实现里三种放大各有多大?怎么改进?

**考察点**:把理论映射到自己的系统。

**参考答案**:
**写放大**:一个 key 的写入会经过 WAL → memtable → flush(L0)→ L0→Ldst → ... → Lmax 的每一层压缩,7 层每层至少一次重写 = 写放大约 7×(加上 L0 可能多次)。大 value 因为只搬指针,实际写放大约 7×12B,极小。
**读放大**:一次 Get 潜在访问 = memtable + immutable + L0(NumLevelZeroTables=5)+ L1..L6 各 1 = 最多约 13 次;布隆把不存在的 key 短路,所以真实读放大远低。
**空间放大**:旧版本 + 墓碑保留到压缩,加上 `NumVersionsToKeep=1` 的保守水位,峰值空间放大约等于"一个 key 在回收前的版本数"。vlog 侧还有失效 value 直到 GC 才回收,`discardRatio` 阈值决定了回收延迟。

**改进方向**:
- **写放大**:① 末层用更大文件(`TableSizeMultiplier=2` 已在做);② 动态 level 大小;③ L0→L0 就地合并(已实现但曾死代码);④ 引入 LSM 层的 key 过滤(zone map)减少不必要压缩。
- **读放大**:① 每层加**分区/更多布隆**(目前每表一个布隆);② 更激进的块缓存策略;③ memtable 换 B+ 树降低随机点读延迟(Q71 的结论)。
- **空间放大**:① 更及时的水位推进(减少 `NumVersionsToKeep` 的保守窗口);② 更主动的 vlog GC 触发;③ 引入压缩(本项目无数据压缩,是最明显的缺失功能)。

**追问**:三个放大能同时优化吗?→ **不能**,这是 LSM 的基本权衡三角。调大 level multiplier 降低写放大但增大读放大;更频繁压缩降低空间放大但增大写放大。工程的本质是**根据负载选一个点**。

---

### Q91. 如果负载是"热点 key 反复更新",你的设计哪里会先崩?

**考察点**:压力推演,开放但有明确答案。

**参考答案**:
先崩的是**读取到旧版本的路径和写放大**,具体链条:
1. **同一 key 在同一 memtable 里反复写**——跳表 `Put` 对同 key+ts 会原地更新,但每次提交 `commitTs` 都不同 → **每次写都新增一个节点**(不同 ts 编码)。热点 key 在 memtable 里会堆积大量版本,`EstimateEntryMemSize` 累加 → 很快触发轮转 → **memtable 频繁轮转,flush 出一个很小的 L0 表**;
2. L0 表数快速逼近 `NumLevelZeroTables=5` → 触发 L0→Ldst 压缩 → 每次压缩都要带上这个热点 key 的所有版本 → **写放大急剧上升**;
3. 读路径:同 key 多版本在 memtable 里,`findNear` 返回最新版本没问题,但迭代器要跳过大量旧版本;压缩回收要等水位推进,`NumVersionsToKeep=1` 只保留 1 版(好),但**水位推进取决于最老的活跃读者**,长事务会拖住回收 → 空间放大;
4. 如果开了冲突检测,**热点 key 的读写事务冲突率会很高**,大量 `ErrConflict` 重试(本项目 RMW 重试上限 200)。

**改善手段**:
- **合并小 flush**:热点场景下 `WaitFlushMemTables`/`NumLevelZeroTables` 的默认值偏小,应该调大以吸收写入;
- **key 级的写缓冲**:同一 key 的连续写在进入 memtable 前合并(但会破坏 WAL 顺序和事务语义);
- **热点探测**:对高频 key 走特殊路径(类似 Badger 的 value log 或专门的 cache);
- **事务层**:对热点 key 用悲观锁或排队,避免冲突重试风暴。

**追问**:这个问题本质是什么?→ **LSM 对"点更新密集"负载天然不友好**,因为每次更新都是"追加新版本"而不是"原地改"。这是 LSM 用"顺序写"换来的,所有 LSM 引擎都有这个特性,只是缓解手段不同。

---

### Q92. 极小 `ValueThreshold` 下 finTxn 被写进 vlog,为什么会导致库打不开?

**考察点**:参数 + 边界组合的 bug(commit 4675bbc)。

**参考答案**:
finTxn 的 value 是 **commitTs 的十进制明文**(Q15),比如 "1" 或 "12"。`ShouldWriteValueToLSM` 的判断是 `len(value) < ValueThreshold`:
- 当 `ValueThreshold ≤ 1` 时,`len("1") < 1` 为 false → finTxn 被判为"大 value" → 它的 value 被换成 **12 字节 `ValuePtr`**,WAL 里只剩指针;
- 重开时 `recovery2SkipList` 对 finTxn 的 value 直接 `strconv.ParseUint(string(entry.Value), 10, 64)`,拿到的是 12 字节二进制指针 → **ParseUint 失败** → `ErrBadTxnFin` → **库无法重开**。

修复:`ShouldWriteValueToLSM` 对 `BitFinTxn` **强制返回 true**,让 finTxn 永远内联。回归测试 `TestReopenTinyValueThreshold`(`ValueThreshold = 1`,写 12 条让 commitTs 递增到两位数)。

**追问**:这个 bug 的教训是什么?→ **"特殊语义的记录"不能被通用规则处理**。finTxn 的 value 不是用户数据、而是引擎内部的元数据,它必须遵循自己的格式约束。通用的"按大小决定存哪"对一个"value 必须可被 ParseUint 解析"的记录是无效规则。修法是给这类记录加**强制例外**并写注释说明原因(代码里注释明确写了"否则重开必然 ErrBadTxn")。

---

### Q93. 为什么用 mmap 而不是 `read` 系统调用?

**考察点**:底层 IO 的取舍。

**参考答案**:
**选 mmap 的理由**:
- **零拷贝读**:`Table.getBlock` 和 `vlog.Read` 直接返回映射切片的视图,没有"内核缓冲 → 用户缓冲"的拷贝;
- **大 value 顺序追加写**方便:直接 `copy` 进 `Buf[offset:]` 再就地算 CRC;
- **就地 CRC**:校验和可以对映射内存直接算,不用额外缓冲。

**代价与应对**:
- **整区 `msync` 代价高**(要遍历大量干净页表项)→ 项目实现了增量同步 `SyncRange/SyncDirtyRange`,WAL 只同步新增前缀(`[lastSynced, writeAt)`),起始页对齐 `off &^ (pagesize-1)`;
- **`mremap` 会搬地址** → 扩容必须与读互斥(Q80 的 SIGSEGV);
- **Windows 必须先 truncate 到映射大小**才能映射;
- **`munmap` 要求长度一致**(Linux 下 `unmmap` 有校验);
- **mmap 的缺页中断在第一次访问时发生**,冷读路径的延迟不像 read 那样可预期(本项目用 `MADV_SEQUENTIAL` 提示顺序访问)。

**追问**:什么时候 read 更好?→ 小文件、随机小 IO 多、或者不适合长期映射的场景(比如文件数极多、虚拟地址空间紧张)。另外 mmap 在"写入后立刻读同一页"时会有 TLB/page fault 开销,`read` 反而更直接。本项目的场景(SST 块 + vlog 顺序读)适合 mmap。

---

### Q94. 跨平台 mmap 实现有哪些坑?

**考察点**:可移植性工程。

**参考答案**:
按 build tag 分三套(见 `mmap/mmap_{linux,darwin,windows}.go`):
- **Linux**:`unix.Mmap(MAP_SHARED)`;`remmap` 用 `SYS_MREMAP` + `MREMAP_MAYMOVE`(**可能搬移映射**);`MADV_SEQUENTIAL`;`msync` 用 `MS_SYNC`。`unmmap` 有 `len == cap` 校验。
- **Darwin**:`remmap` **直接 `return data, nil`——不支持 mremap,size 参数被忽略**。所以扩容语义与 Linux 不同,上层不能假设"Truncate 后映射一定变大";`madvise` 要走 `syscall.Syscall(SYS_MADVISE)`(unix 包不支持)。
- **Windows**:不能映射超过文件实际大小 → 必须先 `fd.Truncate(size)`;用 `CreateFileMapping` + `MapViewOfFile`;`msync` 用 `FlushViewOfFile`;`madvise` 全部 no-op;`remmap` 返回 `nil, nil`(不支持)。

**上层的应对**:
- `file/mmapFile_linux.go` 用 `Sync → ftruncate → Mremap(MAYMOVE)`;非 Linux(`mmapFile_defalut.go`)用 `Sync → Unmap → ftruncate → Mmap`(先解映射再重映射);
- 所有依赖"扩容后地址有效"的代码必须有锁保护(Q80);
- Windows 的预分配行为不同,所以"空 WAL"那类边界(Q19)在不同平台的触发条件也不同。

**追问**:怎么保证跨平台正确?→ 本项目主要在 Linux/WSL2 上开发测试,`windows`/`darwin` 分支是编译期保证(能编过)+ 逻辑对齐(复用同一份 `mmapFile` 抽象)。**诚实地说:非 Linux 平台没有充分测试**,这是我在 `comparison.md` 里应该列出的局限。生产级做法需要在 CI 上跑三平台。

---

### Q95. 这个项目最大的缺点是什么?

**考察点**:必考的自我认知题。

**参考答案**:
按严重程度:
1. **没有数据压缩**。这是最明显的功能缺失——RocksDB/Badger 支持 LZ4/Snappy/ZSTD,压缩能显著降低空间放大和磁盘 IO。本项目 SST 是原始存储,这是"没做"而不是"做错了"。
2. **压缩调度太弱**。50s 定时器 + 分数,不是事件驱动,突发写入时 L0 积压响应慢;并发压缩的冲突检测有,但没有全局调度优先级。
3. **单写者**。写吞吐上限受限于一个核,混合负载下对比 pebble 就吃这个亏(workload a 0.62x)。
4. **零生产验证**。所有测试和基准都在单机、可控参数下,没有真实流量、没有长时运行的稳定性数据、没有大规模数据(>百 GB)的表现。
5. **文档与代码有出入**。比如 README 写 `SyncWrites (true)` 实际是 `false`,`design-0to1.md` 附录写 `ValueLogMaxEntries=1000` 实际是 `10000`。这类不一致是可修复的,但反映出口径没有单一来源。
6. **跨平台未充分测试**(Q94)。

**追问**:你打算先修哪个?→ 按"性价比"排序:**先修文档不一致(零成本)+ 把压缩调度改成事件驱动(收益明确、改动可控),再考虑数据压缩(收益大但工作量大)**。至于单写者,那涉及架构级重设计,不是短期能动的,我会先给出权衡分析而不是贸然改。

---

### Q96. 如果要把它做成分布式,你会怎么改?

**考察点**:系统设计延伸。

**参考答案**:
按"最小可行"到"完整"分层:
1. **分片(Sharding)**:按 key range 把多个 TrainKV 实例组成集群。需要:① key → 分片的元数据(region/apartition)管理,类似 TiKV 的 PD;② region 分裂/合并(数据增长时自动 split);③ 客户端路由(或 proxy 层)。
2. **复制(Replication)**:每个 region 用 Raft(或 Multi-Paxos)做多副本。**关键改造点是 WAL**——现在 WAL 是"本地写本地 fsync";分布式下要先把写请求提交到 Raft 日志(多数派落盘)才算持久。所以 `WriteRequest` 之前要插入一层共识。
3. **时间戳(TSO)**:现在的发号器是**单机单调计数器**,分布式下必须换成**全局单调**:① 中央 TSO(TiDB 做法,单点分配 + 批量预取);② 混合逻辑时钟(HLC);③ 每个分片自己的本地计数器 + 跨分片比较(复杂,需保证全局序)。因为 MVCC 正确性依赖"commitTs 全局有序",这是最关键的改造。
4. **事务**:单机事务是"全局计数器 + 本地冲突检测"。分布式下需要**两阶段提交(2PC)**或 Percolator 模型(primary lock + 二次提交)。本项目的 `finTxn` 结束标记恰好是 Percolator primary lock 的原型——**finTxn 的 key 可以升级为分布式事务的 primary**。
5. **读一致性**:`commitMark` 的水位要跨节点协调,或者改成"读时向各副本确认已提交的最大 commitTs"。

**追问**:哪一步最难?→ **TSO**。它决定了整个 MVCC 的正确性边界;而且它和分片、复制都有耦合(全局单调 vs 本地低延迟是根本矛盾)。TiDB 用中央 TSO + 批量预取(一次给一批 ts)来缓解,这是工程上被验证过的路径。

---

### Q97. 如果重新来过,你会改什么?

**考察点**:复盘深度,避免"什么都不改"的答案。

**参考答案**:
三件事:
1. **memtable 的索引结构**:纯跳表随机点读约 1.8µs,60%+ 时间在 `findNear` 的串行内存延迟链(Q71)。如果能重来,我会评估 **B+ 树 memtable**(像 RocksDB 的 `SkipList` vs `HashSkipList` vs `Vector` 的选择),因为点读的 cache 局部性更好。代价是并发插入的复杂度上升。**但我不后悔先做跳表**——它让整个写路径和读无锁的前提非常干净,是"先能跑对再谈快"的合理顺序。
2. **压缩调度做事件驱动**:现在 50s ticker 是被动的。应该"flush 完一个 L0 表就触发一次 L0 分数检查",外加后台兜底定时器。改动不大,收益明确。
3. **更早引入全量校验测试**:幽灵 key 的 bug 如果是第 10 个测试(全量 10 万 key 读回比对)就抓到,而不是第 30 个,会省很多时间。**"全量比对"应该是最早写的测试之一**,因为 LSM 的正确性问题几乎都是"少量 key 静默丢失",抽样测试命中不了。

**追问**:有什么是你会保留的?→ **kv 分离、W-TinyLFU、水位结构(LimitMark/AtomicMark)、以及"用测试逼死代码暴露"的纪律**。这几个是项目的骨架,重来也会这么做。

---

### Q98. 你怎么向别人证明"这个引擎不丢数据"?

**考察点**:正确性论证的表达能力。非常关键的一题。

**参考答案**:
分三层证明,缺一不可:
1. **机制证明(为什么不会丢)**:
   - 写入路径"值先于引用"(Q55):vlog 先持久化,指向它的 WAL 指针后持久化;
   - WAL 记录带 CRC,撕裂写靠 CRC + 全零判定回退(Q14);
   - 事务原子性靠 `BitTxn`+`BitFinTxn`,未配对的写从不进跳表(Q15);
   - 元数据先 MANIFEST 后挂表(Q18);
   - SST 引用计数保证读时不被删(Q60);
   - 每个机制对应一个不变量,我要能**逐条指出代码位置**。
2. **实验证明(为什么相信)**:
   - **崩溃注入**:`crash_test.go` 用子进程 `os.Exit(0)` 模拟掉电,两种场景(干净崩溃、撕裂 WAL 尾),重开逐 key 校验;
   - **全量校验**:`TestFlushReadIntegrity` 10 万 key 全量读回,misses 必须为 0;`TestDBAutoCompaction` 在压缩期间并发读;`TestValueLogGCFull` 在 GC 期间并发读并重启后再校验;
   - **重复运行**:`-race` + `-count` 多次。
3. **边界证明(为什么相信我没想到的情况也安全)**:
   - 主动构造边界测试:空 WAL、极小阈值、空事务、多次轮转后重开、vlog mmap 扩容与并发读、GC 与并发迭代器;
   - **承认未知**:没有形式化验证,没有 TLA+ 模型,没有 7×24 长跑,没有断电硬件级测试。所以我的证明强度是"机制论证 + 工程测试",不是"形式证明"。

**追问**:哪一条最弱?→ **"边界证明"最弱**,因为它依赖"我能想到哪些边界"。真实系统的失效往往来自没想到的边界组合(比如"极小阈值 + 空事务 + 特定平台"这种三维组合,本项目就同时踩过阈值和空 WAL 两个)。所以这一层的正确表述是"**我覆盖了我能枚举的边界,并列出了未覆盖的部分**",而不是"我覆盖了所有情况"。

---

### Q99. 如果要你 review 一个 LSM 引擎的代码,你会重点看哪几处?

**考察点**:能否把项目经验抽象成方法论。

**参考答案**:
我会按"出错代价"排序,先看**崩溃/静默丢数据**类:
1. **崩溃窗口的原子性**:WAL 记录格式与恢复逻辑是否配对(半写、CRC、截断点)、元数据(MANIFEST)是否先写后挂表、是否有"写了指针但数据没落盘"的窗口;
2. **MVCC 水位与版本回收**:`discardTs` 是怎么算的、有没有"删了还在被读的版本"的可能、水位结构在"连跳/乱序/等待者"下会不会漏唤醒(Q45/Q46);
3. **并发资源生命周期**:SST/vlog 文件在被读时会不会被删(引用计数是否覆盖所有读路径)、引用计数归零判断是否用了原子返回值、关闭顺序是否"先生产者后消费者"(Q76);
4. **比较与编码函数**:`CompareKeyWithTs` 这类函数的前缀/边界语义(Q8 的坑)、变长编码的边界(空 key、超长 key、大于 4GB);
5. **压缩路径覆盖**:是不是所有分支都会被触发(死代码, Q56)、`StaleDataSize` 等关键元数据有没有持久化、墓碑在末层的保留策略;
6. **缓存/索引的一致性**:缓存 key 与内容的映射在"晋升/交换/淘汰"后是否仍然一致(Q59 幽灵 key)、缓存失效与文件删除的配合;
7. **特殊记录的例外规则**:像 finTxn 这种"引擎内部元数据"是否走了不该走的通用路径(Q92);
8. **测试**:有没有全量校验测试、崩溃注入测试、以及"**能失败的**回归测试"。

**追问**:最容易被忽略的是哪个?→ **第 6 和第 7**。因为它们是"局部看起来完全正确的代码"里的错误——缓存晋升逻辑单独看没错,finTxn 的处理单独看也没错,错在**它们与其他不变量/通用规则的交互**。这类问题必须靠"顺着不变量去找谁会破坏它"来发现,不能靠读单个函数。

---

### Q100. 面试官:这个项目最有说服力的部分和最可疑的部分分别是什么?

**考察点**:换位思考,收尾题。也是给自己做最后一遍压力测试。

**参考答案**:
**最有说服力的**:
- **正确性工作的深度和可验证性**。有具体 bug、具体根因、具体回归测试,而且每个 bug 都能说清"为什么测试能抓住它"(幽灵 key 靠全量校验、死锁靠栈分析、死代码靠路径覆盖、水位错误靠构造时序)。这比"实现了 MVCC"有说服力得多,因为它展示的是**发现和修复未知问题的能力**。
- **有量化、有条件、有负结果的性能报告**。1729 ns/op、14 allocs/op、528MB→65KB、缓存 685→361,连同"软件预取负收益"这种失败实验都记录在案。

**最可疑的(我会主动提出,并说明为什么这是"已知局限"而非"隐瞒")**:
- **单机、单参数、零生产验证**:所有数字都来自一台 i5-7300HQ / WSL2。性能结论不能外推到其他硬件;YCSB 对比只代表固定配置下的相对值。
- **正确性论证不完备**:是"机制论证 + 工程测试",不是形式化验证;边界覆盖依赖"我能想到哪些组合"。
- **文档与代码的口径不一致**(SyncWrites、ValueLogMaxEntries),说明缺少单一事实来源。
- **对 Pebble 在混合负载和扫描上落后**(0.62x),这是单写者架构的固有代价,不是调参能解决的。

**怎么收尾**:我会说——"这个项目的价值不在于它是一个能打的生产引擎,而在于它是一份**可验证的工程实践**。我知道它的每一条边界在哪里,知道哪些数字可信、哪些结论不能外推,也知道下一步该做什么。"

**追问**:如果我说"这些都是你自己写的,不可信"怎么办?→ 把验证的入口给面试官:**代码在 GitHub 上,`go test ./... -race -count=1` 可复现,基准脚本在 `benchmk/`,对比报告的原始数据在 `docs/`。哪一条数字你想核,我给命令。** 可复现是最强的可信度证明。

---

## 附录 A:高频"快问快答"清单(面试前 20 分钟过一遍)

| 问题 | 一句话答案 |
|---|---|
| 写路径顺序 | WAL + 跳表;大 value 先 vlog sync 再 WAL sync(值先于引用) |
| 读路径顺序 | pending(读己之写)→ memtable → imm → L0(全部,新表优先)→ L1–L6 各一次二分;每表先布隆再块缓存 |
| key 编码 | `key ‖ BigEndian(MaxUint64-ts)`,使同 key 高版本排在前面 |
| `startTs`/`commitTs` | `startTs = nextTxnTs-1`(读锁),`commitTs = nextTxnTs`(写锁);无 readTs 字段 |
| 写锁请求减半 | `startTs` 不递增计数器 → 统一读锁;只有 commit 拿写锁 |
| 冲突检测 | 写集双哈希(MemHash+xxhash)/读集 hash1;快照校验式,纯写-写检测不到 |
| 水位回收 | `version <= discardTs` 可删;`discardTs = startMark.doneIndex`;压缩前快照 |
| 两个水位结构 | LimitMark(谁还在读,同 index 可多次 Begin)/ AtomicMark(谁提交完了,一一对应) |
| 两个水位死锁 | 连跳后漏唤醒;压堆后漏 absorb |
| group commit | 通道攒批 900/1000 + 1ms 汇合 + 容量 1 令牌串行化;不用 sync.Cond |
| 三条压缩路径 | L0→Ldst/L0→L0;Ln→Ln+1;Lmax→Lmax(stale≥10MB,龄≥1h) |
| 压缩分数 | L0:表数/5;Ln:(size-delSize)/target;L0 永远参与 |
| 布隆 | ~10 bits/key(fp=0.01),k≈6,对 raw key(去 ts) |
| 块缓存 | W-TinyLFU,16 分片,win 15%,SLRU 内 prob 15%/prot 85%,doorkeeper fp=0.005 + CM-Sketch depth=4 |
| cache key | `fid<<32 | blockIdx`,路由 `hash*0x9E3779B97F4A7C15` 取高 4 位 |
| 幽灵 key | SLRU 晋升交换 element 内容但没更新 map → get 返回别人的 value;修:交换后重写索引 + get 重新解析 |
| 零分配扫描 | memtable arena 视图 + SST chunked arena(64KB)+ MergingIterator 不 SafeCopy |
| 大 value 惰性 | Item 只返回 12B 指针,`Value()` 才读;528MB→65KB |
| 分配优化 | TxnSet 25→14,Parallel 18→8(-56%);总分配 -78% |
| 读优化 | memtable 命中 `version<=startTs` 早返回(+BitTxn 门控):2367→1729 ns/op |
| 崩溃测试 | 子进程 `os.Exit(0)` + 0xDE 撕裂 WAL 尾 |
| YCSB | 跑 a/b/c/e;场景 A(1KB 内联)/B(2MB vlog);对 badger 1.3–2.5x,对 pebble 大 value 2–3x、混合 0.62x |
| 三个放大弱点 | 写放大随层数;读放大受 L0 表数;空间放大靠压缩回收 + vlog GC |
| 最大缺点 | 无数据压缩、压缩调度是 50s 定时器、单写者、零生产验证 |

## 附录 B:可以反问面试官的问题

1. 团队现在用的存储引擎是什么?如果是 RocksDB/Badger,当初选型的决定性因素是什么(生态、性能、运维经验)?
2. 团队的写负载画像更接近哪种——点更新密集、范围扫描密集、还是大 value 为主?这直接决定 LSM 参数该往哪边调。
3. 有没有真实遇到过"静默丢数据"或"性能突然退化"的线上问题?是怎么定位的?
4. 对存储引擎这个方向,团队更看重"原理深度"还是"工程落地 + 稳定性"?两者在这类系统里的权重怎么分配?

## 附录 C:已知的文档/代码不一致(被发现时主动承认,别被问住)

| 位置 | 文档写的 | 代码实际 |
|---|---|---|
| `README.md` | `SyncWrites (true)` | `GetDefaultOpt` 里为 `false` |
| `docs/design-0to1.md` 附录 A | `ValueLogMaxEntries=1000` | `option.go` 为 `10000` |
| 概念表述 | "startTs 与 readTs 分离" | 代码无 `readTs` 字段,`readTs` 概念上等于 `startTs` |

> 这三条要么修文档,要么在面试里用"概念上等价/口径未统一"说清楚。**被面试官先发现,不如自己先说。**
