# go-ycsb 三引擎对比报告：TrainKV vs Badger v4.9.6 vs Pebble

- 日期：2026-09-03
- 工具：go-ycsb（本地工作区修改版；badger 适配器已移植到 v4.9.6，trainkv / pebble 适配器为本工作区新增）
- 被测引擎：
  - **trainkv**：本地源码（`replace => ../TrainKV`），LSM + value log 分离
  - **badger** v4.9.6（dgraph-io/badger/v4），LSM + value log 分离
  - **pebble** v1.1.5（cockroachdb/pebble），纯 LSM（无 value log）

---

## 1. 测试环境与方法

| 项 | 值 |
|---|---|
| 平台 | WSL2（linux 6.18.33.2-microsoft-standard-WSL2 x64） |
| CPU / 内存 | i5-7300HQ（nproc=4，8 逻辑线程）/ 7 GB |
| Go | go1.27.0 |
| 数据盘 | 901 GB 可用（/tmp） |

**统一配置**：
- vlog 阈值：trainkv 与 badger 均设为 **1MB**（`trainkv.value_threshold=1048576`、`badger.value_threshold=1048576`；pebble 无阈值概念，所有 value 始终进 LSM）
- badger 关闭压缩（`badger.compression=none`），与 trainkv / pebble 对齐
- 全部 `sync_writes=false`（各引擎默认一致）
- 行模型：10 fields × RowCodec 编码；每库独立数据目录，load 前 dropdata 清库；两两配对执行、引擎间交替，减少时间偏差
- 负载文件：workloada/b/c 为仓库标准（uniform），workloade 已恢复原版 YCSB 参数（zipfian、maxscanlength=100）
- **测量口径**：仅使用 ops/s（墙钟）。go-ycsb 直方图延迟存在系统性低估（见附录 A），本报告不使用延迟数字

**两个场景**：
- **场景 A：value ≈ 1KB（< 1MB 阈值）** → 三引擎均为"全内联 LSM"布局
  recordcount=100000（~100MB/库），operationcount=50000，threads=16，workloads = a / b / c / e
- **场景 B：value ≈ 2MB（> 1MB 阈值）** → trainkv/badger 触发 value log 分离；pebble 无此机制，2MB 行仍进 LSM
  recordcount=2000（~4GB/库），operationcount=4000，threads=16，workloads = a / c（大 value 下 scan/读多场景带宽成本过高，故未跑 b/e）

---

## 2. 场景 A（value 1KB，全内联）

### 2.1 两两配对原始结果（ops/s，A/B = 前者/后者）

**A1 trainkv vs badger**

| 阶段 | trainkv | badger | A/B |
|---|---|---|---|
| load (INSERT) | 32550 | 24718 | 1.32x |
| a (50%R+50%U) | 55387 | 33076 | 1.67x |
| b (95%R+5%U) | 108992 | 55265 | 1.97x |
| c (100%R) | 108388 | 52615 | 2.06x |
| e (95%scan+5%insert) | 3386 | 1382 | 2.45x |

**A2 trainkv vs pebble**

| 阶段 | trainkv | pebble | A/B |
|---|---|---|---|
| load | 33818 | 27934 | 1.21x |
| a | 53035 | 85910 | 0.62x |
| b | 100834 | 64520 | 1.56x |
| c | 97087 | 107433 | 0.90x |
| e | 3283 | 5267 | 0.62x |

**A3 badger vs pebble**

| 阶段 | badger | pebble | A/B |
|---|---|---|---|
| load | 24084 | 29365 | 0.82x |
| a | 32408 | 85211 | 0.38x |
| b | 50061 | 60896 | 0.82x |
| c | 43161 | 113825 | 0.38x |
| e | 1393 | 5426 | 0.26x |

### 2.2 综合矩阵（每引擎两次配对的均值，ops/s）

| 阶段 | trainkv | badger | pebble | 排序 |
|---|---|---|---|---|
| load | 33.2k | 24.4k | 28.6k | trainkv > pebble > badger |
| a（混合 50/50） | 54.2k | 32.7k | 85.6k | **pebble > trainkv > badger** |
| b（读多 95/5） | 104.9k | 52.7k | 62.7k | **trainkv > pebble > badger** |
| c（纯读） | 102.7k | 47.9k | 110.6k | pebble ≈ trainkv > badger |
| e（scan/insert） | 3.3k | 1.4k | 5.3k | **pebble > trainkv > badger** |

> 注：badger 本会话整体偏低（c 仅 43–53k，此前同配置曾达 70–88k），机器存在跨轮漂移；同轮配对的相对比值更可靠。badger 在 a/c 中读吞吐崩塌最严重（a 读 ≈ 16k，c 读 43–52k）。

---

## 3. 场景 B（value 2MB，trainkv/badger 走 vlog，pebble 内联）

### 3.1 两两配对原始结果（ops/s）

**B1 trainkv vs badger**

| 阶段 | trainkv | badger | A/B |
|---|---|---|---|
| load | 31.5 | 26.9 | 1.17x |
| a | 58.2 | 88.6 | 0.66x |
| c | 207.0 | 253.6 | 0.82x |

**B2 trainkv vs pebble**

| 阶段 | trainkv | pebble | A/B |
|---|---|---|---|
| load | 31.3 | 10.6 | 2.95x |
| a | 28.7 | 14.4 | 1.99x |
| c | 466.8 | 152.4 | 3.06x |

**B3 badger vs pebble**

| 阶段 | badger | pebble | A/B |
|---|---|---|---|
| load | 23.3 | 7.2 | 3.24x |
| a | 61.7 | 12.2 | 5.06x |
| c | 454.1 | 107.6 | 4.22x |

### 3.2 综合结论（B 组跨轮噪声大，以配对内比值为准）

| 对比 | load | a（混合） | c（纯读） |
|---|---|---|---|
| trainkv / badger | 1.17x | 0.66x（badger 优） | 0.82x（badger 优） |
| trainkv / pebble | 2.95x | 1.99x | 3.06x |
| badger / pebble | 3.24x | 5.06x | 4.22x |

**大 value 场景下价值分离（vlog）全面碾压无 vlog 的 pebble**（写 3x、混合 2–5x、纯读 3–4.2x）；trainkv 与 badger 同档次（load trainkv 略优，混合/纯读 badger 略优，差异在噪声范围内）。pebble 将 2MB 行写入 LSM 的写放大与碎片化块读取（4KB block 组装 + 校验）是主要劣势。

---

## 4. 观察与结论

1. **阈值语义**：场景 A 是"三个 LSM 引擎全内联"的引擎对比；场景 B 是"vlog 分离 vs 无分离"的**架构**对比（对同一引擎，1KB 阈值下 vlog 化 vs 内联的差异见此前实验）。
2. **小 value（1KB）**：没有赢家通吃——pebble 在混合写（a）与 seek-scan（e）占优，trainkv 在 load 与读多负载（b）占优，badger 本轮全面落后且混合负载读崩塌最甚。
3. **大 value（2MB）**：vlog 分离的价值非常显著；trainkv 与 badger 两个 vlog 引擎互有胜负但同档，pebble 因无大 value 通道全面落后 2–5 倍。
4. **trainkv 的定位**：小 value 读多负载是其强项（b/c 领先或持平），混合写负载弱于 pebble（单车道写 + 每事务同步开销，见此前 CPU profile 分析）；大 value 场景与 badger 同级。

---

## 5. 复现

```bash
# 场景 A（1KB）：阈值 1MB、全内联
RECORDCOUNT=100000 OPERATIONCOUNT=50000 WORKLOADS="a b c e" PROP_DIR=/tmp/props-1k \
EXTRA_A="-p trainkv.value_threshold=1048576" \
EXTRA_B="-p badger.value_threshold=1048576 -p badger.compression=none" \
bash tool/ab_bench.sh trainkv badger        # 及 trainkv pebble / badger pebble

# 场景 B（2MB）：行 > 1MB 阈值触发 vlog
RECORDCOUNT=2000 OPERATIONCOUNT=4000 WORKLOADS="a c" PROP_DIR=/tmp/props-2mb \
...同上...
```

原始逐阶段输出日志（/tmp/ab-logs/）已随 /tmp 清理删除；本报告表格数据完整保留，可随时按第 5 节命令复现。

---

## 附录 A：测量层已知问题

go-ycsb 直方图延迟存在系统性低估（与墙钟节奏对比低估 2–20 倍、倍率随线程数变化），故本报告全部结论基于 ops/s（墙钟）。跨轮噪声 ±10–100%（场景 B 小样本尤甚），**同轮配对内的比值最可靠**；跨轮绝对值对比需多轮取中位数。
