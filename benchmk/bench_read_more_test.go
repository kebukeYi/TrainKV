package benchmk

// 补充读基准: 按"数据所在存储层"划分读取场景;
//
// 场景矩阵:
//   - 数据在 memtable (内存跳表): 已有 HitSeq/HitRandom/Miss/Iterate, 此处不再重复;
//   - 数据在 SST (LSM 磁盘): HitSeqSST / HitRandomSST / HitRandomSSTNoCache / MissSST / IterateSSTEager;
//   - 数据在 vlog (大 value, mmap 拷贝): BigValueRandom (已有 BigValue 顺序版);
//   - 混合: HitRandomMixed (一半 memtable + 一半 L0), BigValueSST (指针在 SST, 值在 vlog);
//
// 说明:
//   - 装载阶段强制 SyncWrites=false, 只影响装载耗时, 与测读无关 (ResetTimer 之后才计时);
//   - SST 场景用 Rotate()+等待 flush 落盘, 使点读/扫描真正走 LSM 读路径 (bloom + 块缓存 + 块解码);
//   - CacheNums=1 用于近似关闭块缓存, 观察"未命中缓存"的裸块读成本;

import (
	"math/rand"
	"os/exec"
	"testing"
	"time"

	"github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/lsm"
)

// openReadDB 打开读基准专用 DB; 装载不落盘, cacheNums<=0 时用默认缓存;
func openReadDB(b *testing.B, cacheNums int) *TrainKV.TrainKV {
	clearDir(perfDataDir)
	opt := lsm.GetDefaultOpt(perfDataDir)
	opt.SyncWrites = false // 装载阶段与测读无关, 关闭刷盘加速准备;
	if cacheNums > 0 {
		opt.CacheNums = cacheNums
	}
	train, err, cleanup := TrainKV.Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = train.Close(); _ = cleanup() })
	return train
}

// loadDataRange 装载 [start, start+count) 号 key, 每批 perfBatch 条提交;
func loadDataRange(b *testing.B, train *TrainKV.TrainKV, start, count int) {
	txn := train.NewTransaction(true)
	for i := start; i < start+count; i++ {
		if err := txn.Set(GetKey(i), GetValue()); err != nil {
			b.Fatal(err)
		}
		if (i-start)%perfBatch == 0 {
			if _, err := txn.Commit(); err != nil {
				b.Fatal(err)
			}
			txn = train.NewTransaction(true)
		}
	}
	if _, err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
}

// waitFlushToL0 等待 flush 完成: L0 表数量达到 wantTables 且连续若干轮不再增长;
// 读路径会同时覆盖 memtable/immem/L0, 但"SST 场景"希望计时开始时数据已稳定落盘;
func waitFlushToL0(b *testing.B, train *TrainKV.TrainKV, wantTables int) {
	deadline := time.Now().Add(30 * time.Second)
	var lastCount, stable int
	for time.Now().Before(deadline) {
		c := len(train.Lsm.LevelManger.GetLevelHandler(0).GetTables())
		if c >= wantTables && c == lastCount {
			stable++
			if stable >= 3 {
				return
			}
		} else {
			stable = 0
		}
		lastCount = c
		time.Sleep(100 * time.Millisecond)
	}
	b.Fatalf("wait flush to L0 timeout: tables=%d want>=%d", lastCount, wantTables)
}

// loadSSTData 装载 100K 条并全部刷入 L0, 返回数据全在 SST 的 DB;
func loadSSTData(b *testing.B) *TrainKV.TrainKV {
	train := openReadDB(b, 0)
	loadDataRange(b, train, 0, perfKeyNum)
	train.Lsm.Rotate()
	waitFlushToL0(b, train, 1)
	return train
}

// ---------------- 数据在 SST ----------------

// BenchmarkReadGetHitSeqSST 数据全部在 L0 层的 SST, 顺序点读;
// 333          37943293 ns/op        17369433 B/op     228316 allocs/op
func BenchmarkReadGetHitSeqSST(b *testing.B) {
	b.ReportAllocs()
	train := loadSSTData(b)
	txn := train.NewTransaction(false)
	defer txn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := txn.Get(GetKey(i % perfKeyNum)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadGetHitRandomSST 数据全部在 SST, 随机点读 (bloom + 块缓存);
// 359989	      2999 ns/op	     758 B/op	       7 allocs/op
func BenchmarkReadGetHitRandomSST(b *testing.B) {
	train := loadSSTData(b)
	order := rand.New(rand.NewSource(42)).Perm(perfKeyNum)

	b.ReportAllocs()
	txn := train.NewTransaction(false)
	defer txn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := txn.Get(GetKey(order[i%perfKeyNum])); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadGetHitRandomSSTNoCache 同上但缓存只有 1 格, 近似无缓存: 每次点读都要做块读+解码;
// 3989263              2984 ns/op             895 B/op          8 allocs/op
func BenchmarkReadGetHitRandomSSTNoCache(b *testing.B) {
	b.ReportAllocs()
	train := openReadDB(b, 1)
	loadDataRange(b, train, 0, perfKeyNum)
	train.Lsm.Rotate()
	waitFlushToL0(b, train, 1)
	order := rand.New(rand.NewSource(42)).Perm(perfKeyNum)
	txn := train.NewTransaction(false)
	defer txn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := txn.Get(GetKey(order[i%perfKeyNum])); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadGetMissSST 未命中 SST: 布隆过滤器直接判负, 跳过块读;
// 14780430               749.2 ns/op           123 B/op          2 allocs/op
func BenchmarkReadGetMissSST(b *testing.B) {
	b.ReportAllocs()
	train := loadSSTData(b)
	txn := train.NewTransaction(false)
	defer txn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := txn.Get(GetKey(perfKeyNum + i)); err != common.ErrKeyNotFound {
			b.Fatalf("expect miss, got err=%v", err)
		}
	}
}

// ---------------- 混合 (memtable + SST) ----------------

// BenchmarkReadGetHitRandomMixed 一半数据在 L0, 一半在 memtable, 随机点读 (完整 Get 路径);
// 4358523              2634 ns/op             407 B/op          4 allocs/op
func BenchmarkReadGetHitRandomMixed(b *testing.B) {
	b.ReportAllocs()
	train := openReadDB(b, 0)
	half := perfKeyNum / 2
	loadDataRange(b, train, 0, half)
	train.Lsm.Rotate()
	waitFlushToL0(b, train, 1)
	loadDataRange(b, train, half, perfKeyNum-half)
	order := rand.New(rand.NewSource(42)).Perm(perfKeyNum)
	txn := train.NewTransaction(false)
	defer txn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := txn.Get(GetKey(order[i%perfKeyNum])); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------- 数据在 vlog ----------------

// BenchmarkReadGetBigValueRandom 1MB 大 value (存 vlog, LSM 只存 ValuePtr), 随机读;
// 104901            108631 ns/op         1056920 B/op          5 allocs/op
func BenchmarkReadGetBigValueRandom(b *testing.B) {
	b.ReportAllocs()
	train := openReadDB(b, 0)
	val := make([]byte, 1<<20+1) // 1MB+1 > ValueThreshold(1MB);
	txn := train.NewTransaction(true)
	const n = 500
	for i := 0; i < n; i++ {
		if err := txn.Set(GetKey(i), val); err != nil {
			b.Fatal(err)
		}
		if i%50 == 0 {
			if _, err := txn.Commit(); err != nil {
				b.Fatal(err)
			}
			txn = train.NewTransaction(true)
		}
	}
	if _, err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
	order := rand.New(rand.NewSource(42)).Perm(n)
	rtxn := train.NewTransaction(false)
	defer rtxn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rtxn.Get(GetKey(order[i%n])); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadGetBigValueSST 大 value 的指针在 SST (数据刷盘后), 值在 vlog: 完整跨层路径;
// 93573            111329 ns/op         1056989 B/op          8 allocs/op
func BenchmarkReadGetBigValueSST(b *testing.B) {
	b.ReportAllocs()
	train := openReadDB(b, 0)
	val := make([]byte, 1<<20+1)
	txn := train.NewTransaction(true)
	const n = 500
	for i := 0; i < n; i++ {
		if err := txn.Set(GetKey(i), val); err != nil {
			b.Fatal(err)
		}
		if i%50 == 0 {
			if _, err := txn.Commit(); err != nil {
				b.Fatal(err)
			}
			txn = train.NewTransaction(true)
		}
	}
	if _, err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
	train.Lsm.Rotate()
	waitFlushToL0(b, train, 1)
	order := rand.New(rand.NewSource(42)).Perm(n)
	rtxn := train.NewTransaction(false)
	defer rtxn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rtxn.Get(GetKey(order[i%n])); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------- 扫描 ----------------

// BenchmarkReadIterateSSTEager 全量扫描 SST 并取 value (已有 IterateSST 只计数不取值);
// 333          37943293 ns/op        17369433 B/op     228316 allocs/op
func BenchmarkReadIterateSSTEager(b *testing.B) {
	b.ReportAllocs()
	train := loadSSTData(b)
	txn := train.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: false})
	defer func() { _ = iter.Close() }()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for iter.Rewind(); iter.Valid(); iter.Next() {
			if _, err := iter.Item().Value(); err != nil {
				b.Fatal(err)
			}
			count++
		}
		if count != perfKeyNum {
			b.Fatalf("scan count=%d, want %d", count, perfKeyNum)
		}
	}
}

// ---------------- 冷读 (drop 页缓存后真正读盘) ----------------
//
// 用法: 固定迭代数单遍访问, 让每个数据块只从磁盘读一次;
//   BenchmarkReadColdScanSST:       -benchtime=1x        (单遍顺序扫描, 每块读一次)
//   BenchmarkReadColdRandomSST:     -benchtime=100000x   (单遍随机点读 100K key)
//   BenchmarkReadColdBigValueRandom: -benchtime=500x     (单遍随机读 500 个 1MB vlog 值)
// 注意: 需要 root 权限; 随机冷读磁盘延迟大, 用固定迭代数避免标定阶段把结果稀释成热读;

// dropPageCache 同步脏页后清空 OS 页缓存 (drop_caches 只回收干净页), 需 root;
func dropPageCache(b *testing.B) {
	out, err := exec.Command("sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches").CombinedOutput()
	if err != nil {
		b.Fatalf("drop page cache failed: %v, out=%s", err, out)
	}
}

// BenchmarkReadColdScanSST 冷读顺序全扫描: 数据在 L0, 清缓存后单遍扫描;
// 391          29543881 ns/op         4569440 B/op     128316 allocs/op
func BenchmarkReadColdScanSST(b *testing.B) {
	b.ReportAllocs()
	train := loadSSTData(b)
	dropPageCache(b)
	txn := train.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: false})
	defer func() { _ = iter.Close() }()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for iter.Rewind(); iter.Valid(); iter.Next() {
			_ = iter.Item()
			count++
		}
		if count != perfKeyNum {
			b.Fatalf("scan count=%d, want %d", count, perfKeyNum)
		}
	}
}

// BenchmarkReadColdRandomSST 冷读随机点读: 清缓存后单遍随机访问全部 key;
// 4009574              2968 ns/op             751 B/op          6 allocs/op
func BenchmarkReadColdRandomSST(b *testing.B) {
	b.ReportAllocs()
	train := loadSSTData(b)
	order := rand.New(rand.NewSource(42)).Perm(perfKeyNum)
	dropPageCache(b)
	txn := train.NewTransaction(false)
	defer txn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := txn.Get(GetKey(order[i%perfKeyNum])); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadColdBigValueRandom 冷读大 value: 清缓存后单遍随机读 vlog 中的 1MB 值;
// 107120            108588 ns/op         1056920 B/op          5 allocs/op
func BenchmarkReadColdBigValueRandom(b *testing.B) {
	b.ReportAllocs()
	train := openReadDB(b, 0)
	val := make([]byte, 1<<20+1) // 1MB+1 > ValueThreshold(1MB);
	txn := train.NewTransaction(true)
	const n = 500
	for i := 0; i < n; i++ {
		if err := txn.Set(GetKey(i), val); err != nil {
			b.Fatal(err)
		}
		if i%50 == 0 {
			if _, err := txn.Commit(); err != nil {
				b.Fatal(err)
			}
			txn = train.NewTransaction(true)
		}
	}
	if _, err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
	order := rand.New(rand.NewSource(42)).Perm(n)
	dropPageCache(b)
	rtxn := train.NewTransaction(false)
	defer rtxn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rtxn.Get(GetKey(order[i%n])); err != nil {
			b.Fatal(err)
		}
	}
}
