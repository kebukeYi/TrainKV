# TrainKV 面试准备:追问清单与参考答案

> 配套简历陈述与模拟面试审查见文末。所有答案基于本项目实际实现(非泛泛而谈),
> 面试前请对照代码逐条理解,能脱稿复述。

---

## 一、LSM 基础

### Q1. 为什么 LSM 写快读慢?三个放大是什么?

**参考答案**:
- 写快:写入只做**顺序追加**——进内存跳表(memtable)+ 追加 WAL,配合 group commit 摊销 fsync,没有随机写、没有原地更新;
- 读慢:读要**逐层查**——memtable → 不可变表 → L0(全部表,逆序)→ L1..L6(每层二分定位一张表),还要处理多版本;
- **写放大**:一个 key 被反复写——WAL 一次、flush 一次、每层压缩再写一次;本项目 L0-L6 共 7 层,理论写放大 ≈ 层数 × 各层参与压缩的次数;
- **读放大**:一次 Get 的磁盘访问次数——L0 表数 + 每层 1 次二分;布隆过滤器把不存在的 key 直接短路(本项目 L0 上限 5 张表 + bloom);
- **空间放大**:被覆盖的旧版本和删除标记直到压缩才被清理,期间占用空间。

**加分点**:能说出"压缩就是拿写放大换读放大和空间放大"的权衡本质。

### Q2. 为什么 memtable 用跳表不用 B+树?

**参考答案**:
- 跳表 O(log n) 查找/插入,无需再平衡;节点间原子指针,**读可以无锁并发**,写用 CAS——本项目写路径被写通道串行化,读却可能来自任意协程,跳表天然适配;
- 节点分配走 arena(连续内存、一次大分配),避免逐节点 malloc 的碎片与 GC 压力;
- B+树(如 RocksDB 的并发 memtable)需要复杂的并发控制,实现成本高。

### Q3. L0 层为什么允许 key 重叠?L0→L1 压缩为什么选多张表?

**参考答案**:
- 相邻 memtable 的 key 范围天然重叠(不同时刻写同一批 key);flush 时直接倾倒 memtable 不做排序归并,所以 L0 允许重叠——**用读放大换写路径简单**;
- L0 读要查全部表(逆序、新表优先,命中即停),所以 L0 表数要控制(NumLevelZeroTables=5);
- L1+ 每层表范围互不重叠,读只需每层一次二分;
- L0→L1 压缩把多张重叠表归并成排序后的 L1 表,同时清掉被覆盖的旧版本。

---

## 二、事务与 MVCC

### Q4. startTs / commitTs 怎么分配?为什么读事务要等水位?

**参考答案**:
- `TransactionManager.nextTxnTs` 全局计数器(tsLock 保护):`startTs = nextTxnTs - 1`,`commitTs = nextTxnTs` 然后自增——时间戳即全局提交序;
- 读事务开启时 `commitMark.WaitForIndexDone(startTs)`:等待"所有 ≤ startTs 的提交都完成"——因为提交序与时间戳序一致,等水位保证读事务看到它开始前的全部已提交数据;
- 写事务延迟等待(`ensureStartTsReady` 首次读 LSM 时才等),纯写事务不付这笔等待;
- `startMark` 跟踪谁还在读(指导版本回收),`commitMark` 跟踪谁提交完了(保证读可见性);`LimitMark` 是水位推进器(begin/done 事件 + 等待者队列)。

### Q5. 冲突检测怎么做?为什么注册两个哈希?

**参考答案**:
- `DetectConflicts` 开启时:每次 Set 把 key 的**两个独立哈希**(MemHash + xxhash)登记进 `conflictKeys`;每次 Get 把 MemHash 记入 `readKeys`;
- 提交时 `hasConflict`:本事务 startTs 之后提交的事务中,若有任一 write key 哈希命中本事务的 read key → 冲突,返回 ErrConflict(乐观并发控制);
- 两个哈希是为了**消除单 64 位哈希碰撞的误报**——两个独立哈希同时碰撞的概率可忽略。

### Q6. 压缩为什么只清理 `version <= discardTs` 的版本?

**参考答案**:
- MVCC 安全:startTs=T 的读事务必须能看到所有 ≤T 的版本;若压缩删了某个 ≤T 的版本而该事务还在读,就会读到"历史被篡改";
- `discardTs` = 已结束读事务的最高 startTs(startMark 完成水位)——版本 ≤ discardTs 意味着**不可能再有活跃读者需要它**,可以安全回收;
- 压缩归并时对同一 key 保留 `NumVersionsToKeep`(1)个最新版本,其余丢弃。

---

## 三、崩溃恢复

### Q7. WAL 事务标记与半条撕裂写怎么处理?

**参考答案**:
- 每个事务的条目带 `BitTxn` 标记 + 一条 `finTxn` 结束标记(key 为 `!train!txn!fin`,value 为 commitTs),同事务条目连续;
- 恢复(`recovery2SkipList`)顺序读 WAL:遇到 BitTxn 收集并校验同事务号;遇到 BitFinTxn 校验 value 与事务号一致后一次性入跳表;任何不一致 → ErrBadTxn,截断到最后一个有效偏移;
- 尾部撕裂写/零填充:读到 EOF/UnexpectedEOF/CRC 不匹配且其后全零 → 视为尾部,回退到最后有效偏移并截断;
- 每条记录有 CRC32 校验。

### Q8. MANIFEST 的作用?为什么先写 MANIFEST 再挂表?

**参考答案**:
- MANIFEST 持久化"表 → 层级"的登记,重启时据此重建层级结构;
- flush 顺序:建 SST 文件 → **写 MANIFEST** → 挂到内存层级;
- 崩溃窗口分析:文件写完但 MANIFEST 没写 → 重启时孤儿 SST 被忽略(manifest 不知道它);MANIFEST 写了但 WAL 没删 → 重启时**fid ≤ 最大 SST fid 的 WAL 被丢弃**(数据已在 SST 里)——两种窗口都不丢数据、不产生重复;
- 压缩同理:先写 MANIFEST 变更,再更新内存层级。

### Q9. 为什么 vlog 要先于 WAL 同步?

**参考答案**:
- LSM 里大 value 条目存的是指向 vlog 的 12 字节指针;若 WAL(含指针)先落盘而 vlog 未同步,断电后指针指向未持久化的数据 → 悬垂读;
- 顺序:写 vlog → **sync vlog** → 写 WAL → sync WAL——保证"值先于引用落盘"。

---

## 四、vlog 与 GC

### Q10. 为什么大 value 分离?ValuePtr 怎么编码?

**参考答案**:
- `ValueThreshold`(默认 1MB):超过的值进 vlog,LSM 只存指针——LSM 体积小 → 压缩 I/O 少、小数据读快、内存表能装更多 key;
- ValuePtr 12 字节 = Fid(4)+ Len(4)+ Offset(4),unsafe 直接拷入字节切片;条目 Meta 置 `BitValuePointer`;
- 读:解码 vp → `vlog.Read` → mmap 视图 → `SafeCopy` 给调用方(调用方持有语义)。

### Q11. GC 为什么依赖压缩统计?与并发读怎么保证安全?

**参考答案**:
- 压缩丢弃旧版本时,若该版本的值是 vlog 指针,累计 `discardStats[vp.Fid] += vp.Len` → 经 FlushCh 汇入 FileMap → `RunValueLogGC(ratio)` 按丢弃比例挑选文件 → `gcReWriteLog` 把存活条目重写到当前文件 → 删除旧文件;
- 并发安全三层:① vlog 文件 RWMutex(读持 RLock,GC 删除持 Lock);② 活跃迭代器计数——有迭代器在用就把文件挂到 `FilesToDel` 延后删除;③ 写路径单写者串行,GC 重写经同一写通道,不会交错;
- "版本不在 LSM(被压缩清掉)"= 该 vlog 条目已死,丢弃而非报错(本轮修复)。

---

## 五、压缩调度

### Q12. 三条压缩路径各自什么时候触发?

**参考答案**:
- **L0→目标层**:L0 得分 = 表数/5 ≥ 1(即 ≥5 张)且调整后分数不在 (0,1);选重叠表归并到 base level(小库即 L6);
- **L0→L0**:L0 得分在 (0,1) 且 ≥4 张小表(单表 < 2×fileSize[0]、表龄 ≥10s)——小表合大,减少 L0 表数;
- **Lmax→Lmax**:末层表 stale ≥ 10MB、表龄 ≥ 1h、MaxVersion ≤ discardTs——回收末层死数据;
- 调度:`pickCompactLevels` 按"当前大小/目标大小"打分,`RunOnce` 按优先级执行,compactor 0 优先 L0;
- **已知缺点(主动说出来)**:调度是 50s ticker,压缩窗口大,生产应事件驱动/参数化——这是 todo 里已列的项目。

---

## 六、优化与工程

### Q13. 零分配扫描怎么实现的?

**参考答案**:
- 合并层不再每 key SafeCopy,子迭代器自产稳定 Item;
- memtable:分块 arena(64KB/块,**块一经分配不移动**,返回切片永久有效;活跃表用副本防并发写导致 arena 扩容悬垂,不可变表用视图);
- SST:key 副本进 arena,value 是 mmap 视图(表存续期间稳定);
- `Rewind` 重置 arena(借用契约:Rewind 前所有 Item 已消费);
- 效果:10 万条扫描从 20 万次分配降到 832 次;SST 扫描 0 分配。

### Q14. 大 value 惰性读的 API 为什么这么设计?

**参考答案**:
- 旧实现每次 `Item()` 都拷贝 1MB;现在 `interfaces.Item` 携带 ValuePtr 位置,`TxnIterator` 只记位置不拷贝,`item.Value()` 按需从 vlog 解码;
- 效果:扫 500×1MB 从必付 528MB 降到只搬 65KB,用户真正取值才付;
- 契约:须在 `Next()` 前消费(借用语义的一部分)。

### Q15. 为什么基准用 allocs/op 而不是只看 ns/op?

**参考答案**:
- 写路径 ns/op 被 fsync 主导,本机(WSL2)噪声 2.6ms~38ms,不可比;
- allocs/op 稳定,且本轮优化的对象就是分配;
- B/op 会被 64MB arena 预分配摊销污染(b.N 不同不可比);
- 方法论:pprof alloc_objects 指路(注意小对象采样偏差)、`-benchmem` 验收、`-gcflags='-m'` 逃逸分析取证。

---

## 七、必问的"软肋"问题

### Q16. 你项目最大的缺点是什么?(主动暴露 + 给出计划)

**参考答案**:
1. **压缩调度是 50s ticker**(最大架构弱点):压缩窗口大 → 写放大/读放大窗口大;计划:事件驱动(写入/水位触发)+ 参数化;
2. **无数据压缩**:裸存储,磁盘占用高;计划:block 级 Snappy/Zstd;
3. **无生产验证**:没有部署、没有与 goleveldb/badger 的同机 A/B 基准;计划:补对比基准;
4. **崩溃恢复测试浅**:只有写入中断电 + WAL 尾部损坏;计划:flush/压缩/GC 中途断电场景。

### Q17. 为什么不用现成的 Badger/RocksDB,要自己写?

**参考答案**:
- 转行学习目的:只有从零实现,才能真正理解每个设计决策——为什么压缩分 level、为什么 kv 分离、为什么 GC 依赖压缩统计;
- 本项目 ~1 万行可通读,是完整 LSM 的教学级实现,而非玩具(事务/GC/恢复/测试齐全);
- 与 Badger 的差异:分配优化更激进(扫描零分配、惰性读)、测试纪律(端到端 + race 全绿)。

---

## 八、简历陈述(定稿版)

> **TrainKV — 从零实现的 LSM 存储引擎(Go,~1 万行)**
> 跳表 memtable + WAL、L0–L6 分层压缩(三条压缩路径)、W-TinyLFU 块缓存、布隆过滤器、kv 分离(vlog)+ 按 discard 统计的垃圾回收、MVCC 事务(TSO + 冲突检测)、MANIFEST 崩溃恢复。
> 性能:写路径 5–8 allocs/op,扫描零分配(分块 arena),大 value 惰性读取;
> 工程:五大链路端到端测试 + race 检测全绿,排查并修复 20+ 隐藏缺陷(含块缓存并发映射错误、三条压缩路径死代码)。

## 九、面试开场白(30 秒)

> "我选择从零写 LSM 而不是直接用 Badger,是为了把存储引擎的每个决策——为什么压缩分 level、为什么 kv 分离、为什么 GC 依赖压缩统计——真正理解到能自己实现的程度。这个项目约一万行,包含事务、压缩、崩溃恢复和端到端测试,我可以从任何一层开始讲。"
