# TrainKV 待做列表

> 按优先级排序。前两项是正确性验证缺口,其余为性能与工程化事项。

## 一、正确性验证缺口(建议优先)

### 1. 崩溃恢复深场景测试
- 现状:`TestCrashRecovery` 只覆盖"写入中断电 + WAL 尾部损坏"(50 keys,浅场景)。
- 待补:
  - flush 中途断电(子进程写满 memtable 后 kill);
  - vlog 轮转中途断电;
  - 压缩中途断电(压缩是长任务,是崩溃窗口最大的环节);
  - vlog GC 后断电;
  - 恢复后校验:数据完整性、MANIFEST 一致性、无幽灵 key。
- 方法:复用现有 `TestCrashRecoveryChild` 的独立进程 + `os.Exit` 模式。

### 2. 大数据量 / 长时运行(soak)测试
- 现状:所有测试数据量 ≤ 数十万 key,运行时间 ≤ 分钟级。
- 待补:
  - GB 级数据写入/压缩,验证层分布与写放大;
  - 长时间(小时级)运行,监控:协程数泄漏、内存增长(arena/缓存/池)、文件句柄泄漏;
  - 反复打开/关闭/重开,验证 manifest 与锁文件回收。

### 3. vlog GC 高压并发压测
- 现状:`TestValueLogGCFull` 覆盖单文件 GC 全链路 + 并发读。
- 待补:
  - 多 vlog 文件、多轮 GC;
  - GC 与写、压缩同时进行(GC 重写依赖 vlog 写通道,验证无死锁/无饥饿);
  - GC 与迭代器并发(迭代器持有 vlog 文件锁的路径);
  - discard 统计与 GC 挑选比率的边界(ratio 极端值、文件恰好等于阈值)。

## 二、性能优化(读路径剩余分配)

### 4. Get 路径 2 次分配
- 现状:每次 `txn.Get` = `KeyWithTs`(key+8 拷贝)+ `db.get` 的 `&entry` 逃逸(72B)。
- 选项(需权衡,涉及公共 API):
  - A. `db.get`/`Transaction.Get` 改返回值 —— 破坏性 API 变更,可彻底消除;
  - B. 维持现状 —— 与 Badger 同策略(Get 返回 `*Item` 同样是堆对象)。
- 附带:`db.get` 大 value 分支的 `var vp model.ValuePtr` 也逃逸(12B/次,仅大 value 路径)。

### 5. searchBlock 的栈上 blockIterator key 拷贝
- 现状:`Search` 路径每次 1 次 key 拷贝(`blockIterator` arena=nil 退回 SafeCopy)。
- 方向:池化 key 缓冲或复用请求级缓冲(需保证调用方消费前不被覆写)。

## 三、性能优化(CPU/架构)

### 6. LimitMark processOn 每提交一个通知协程
- 现状:每次水位推进 `go func() { doneIndexCh <- curIndex }()` 一个协程(约 2KB 栈)。
- 方向:无等待者时直接原子推进 doneIndex 的快速路径;或复用单协程消费者。

### 7. skiplist 插入位置缓存
- 现状:批量有序写时每次从 head 重新查找。
- 方向:写者串行(handleWriteCh 单写者),记住上次插入位置,有序写时从该位置继续。
- 收益:有序写入场景的 CPU 优化;随机写无益。

### 8. vlog 读路径零拷贝确认
- 现状:大 value 读 = mmap 视图 + `SafeCopy` 拷贝(调用方持有语义需要)。
- 方向:若可接受借用语义,提供零拷贝读接口(需评估与 `callBack` 文件锁的生命周期)。

## 四、工程化

### 9. 契约文档化与强制执行
- 已引入的契约(需在 README/API 文档写明):
  - 事务 `Commit`/`Discard` 后不可再使用(池化);
  - 迭代器 Item 为借用语义:`Rewind`/`Seek` 后旧 Item 失效;
  - 大 value 取值用 `item.Value()` 而非 `item.Item.Value`;
  - `item.Value()` 须在 `Next()` 前消费。
- 可考虑:契约违规的 debug 断言(如 `-tags debug` 下检测)。

### 10. 基准回归机制
- 现状:`benchmk/bench_perf_test.go` 有全套基准,但需手动运行。
- 方向:基准脚本(`scripts/bench.sh`)+ 关键指标阈值告警(如 allocs/op 回退 >10% 报警),可接入 CI。

### 11. compaction 参数化
- 现状:`runCompacter` 的 ticker 硬编码 50s;`findTablesL0ToL0` 表龄 10s;`findMaxLevelTables` 表龄 1h、stale 阈值 10MB。
- 方向:以上阈值移入 `Options`,便于测试与运维调参。

### 12. 文档同步
- 现状:`docs/write-path-optimization.md` 记录前两轮写路径优化。
- 待做:将本轮(读路径/正确性修复/压缩路径测试)内容并入,形成完整演进史。

## 五、已知非缺陷项(无需处理)

- 写路径每 op 2 次 fsync(vlog + WAL):SyncWrites=true 的固有成本,group commit 已摊销;
- `WriteRequest` 基准 15 allocs/op:该 API 不走 txn/writeCh 路径,分配已是最低;
- 迭代器 arena 驻留内存:借用契约下的固有取舍,Rewind 时已重置。
