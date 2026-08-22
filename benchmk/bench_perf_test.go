package benchmk

// 全方位读写流程性能基准;
//
// 读流程: memtable 命中(顺序/随机)、未命中(不存在的key)、vlog 大 value、迭代器全量扫描(不进cache);
// 写流程: 串行单条(128B/4KB)、批量 10 条、并发、大 value(1MB, kv 分离)、直写API;
// 混合:   9:1 读:写;

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/lsm"
)

var perfDataDir = "/usr/golanddata/trainkv/perf2"

const (
	perfKeyNum = 100000 // 读数据集规模: 100K * 512B ≈ 50MB, 大于 10MB memtable, 触发轮转;
	perfBatch  = 500    // 装载时的批大小;
)

func openPerfDB(b *testing.B) *TrainKV.TrainKV {
	clearDir(perfDataDir)
	defaultOpt := lsm.GetDefaultOpt(perfDataDir)
	//defaultOpt.MemTableSize = 10 << 20
	defaultOpt.SyncWrites = true
	train, _, _ := TrainKV.Open(defaultOpt)
	b.Cleanup(func() { _ = train.Close() })
	return train
}

// loadData 装载 perfKeyNum 个key(512B value);
func loadData(b *testing.B, train *TrainKV.TrainKV) {
	txn := train.NewTransaction(true)
	for i := 0; i < perfKeyNum; i++ {
		// val: 512B
		if err := txn.Set(GetKey(i), GetValue()); err != nil {
			b.Fatal(err)
		}
		if i%perfBatch == 0 {
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

// ---------------- 写流程 ----------------

// BenchmarkWriteTxnSet 串行事务单条提交, 128B value, 纯写(无读阶段);
func BenchmarkWriteTxnSet128B(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)

	val := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := train.NewTransaction(true)
		if err := txn.Set(GetKey(i), val); err != nil {
			b.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// 并发提交: 多个事务的写请求被 handleWriteCh 攒成一批, 共享一次 fsync (group commit);
// 与串行 BenchmarkTrainKVTxnSet-128B 对比, 观察每 op 摊销的刷盘开销;
func BenchmarkWriteTxnSet128BParallel(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkDir)
	train := openPerfDB(b)
	defer train.Close()

	var counter atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			key := []byte(fmt.Sprintf("key=%d", i))
			txn := train.NewTransaction(true)
			if err := txn.Set(key, make([]byte, 128)); err != nil {
				b.Fatal(err)
			}
			if _, err := txn.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkWriteTxnSet4K 串行事务单条提交, 4KB value(仍低于 1MB 阈值, 走 LSM 直写);
// wal_no_sync ↓
// mem all_obj
// 540230            106961 ns/op           28069 B/op         24 allocs/op
// 修复了 common.Conpainc(bool, error.new())
// 534298             74711 ns/op           26075 B/op         12 allocs/op
// 增加 model.NewEntry() 池化;
// 593797             92589 ns/op           27596 B/op         12 allocs/op
// 340298             70449 ns/op           23898 B/op         10 allocs/op
// wal_sync ↓
//
//	1369           8988261 ns/op             210 B/op          5 allocs/op
//	1375           9227016 ns/op             212 B/op          5 allocs/op
//	1311           8863622 ns/op             118 B/op          4 allocs/op
//	1116           9112040 ns/op             119 B/op          4 allocs/op
func BenchmarkWriteTxnSet4K(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)
	val := make([]byte, 4<<10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := train.NewTransaction(true)
		if err := txn.Set(GetKey(i), val); err != nil {
			b.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteTxnSet4KParallel 并发事务提交; 验证 SyncWrites 下攒批同步的效果:
// 并发提交汇合到同一批次, 一次刷盘覆盖多个提交;
func BenchmarkWriteTxnSet4KParallel(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)
	val := make([]byte, 4<<10)
	var seq int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&seq, 1)
			txn := train.NewTransaction(true)
			if err := txn.Set(GetKey(int(i)), val); err != nil {
				b.Fatal(err)
			}
			if _, err := txn.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------- 读流程 ----------------

// BenchmarkReadGetHitSeq 顺序命中;
func BenchmarkReadGetHitSeq(b *testing.B) {
	train := openPerfDB(b)
	loadData(b, train)
	b.ReportAllocs()
	txn := train.NewTransaction(false)
	defer txn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := txn.Get(GetKey(i % perfKeyNum)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadGetHitRandom 随机命中(预生成随机序, 排除 rand 调用对测时的干扰);
func BenchmarkReadGetHitRandom(b *testing.B) {
	train := openPerfDB(b)
	loadData(b, train)
	b.ReportAllocs()
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

// BenchmarkReadGetMiss 未命中(数据集之外的 key);
func BenchmarkReadGetMiss(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)
	loadData(b, train)

	txn := train.NewTransaction(false)
	defer txn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := txn.Get(GetKey(perfKeyNum + i)); err != common.ErrKeyNotFound {
			b.Fatalf("expect miss, got err=%v", err)
		}
	}
}

// BenchmarkReadGetBigValue 大 value 读: 1MB value 存于 vlog, LSM 存 ValuePtr, 读时经 mmap 拷贝;
func BenchmarkReadGetBigValue(b *testing.B) {
	b.ReportAllocs()
	clearDir(perfDataDir)
	train, _, _ := TrainKV.Open(lsm.GetDefaultOpt(perfDataDir))
	defer train.Close()
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
	rtxn := train.NewTransaction(false)
	defer rtxn.Discard()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rtxn.Get(GetKey(i % n)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadIterate 迭代器全量顺序扫描(每次 op = 扫描完整数据集);
func BenchmarkReadIterate(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)
	loadData(b, train)
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

// ---------------- 混合 ----------------

// BenchmarkMixedRead90Write10 9:1 读写混合: 90% 读已有 key, 10% 提交新 key;
func BenchmarkMixedRead90Write10(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)
	loadData(b, train)
	val := make([]byte, 128)
	rtxn := train.NewTransaction(false)
	defer rtxn.Discard()
	nextWrite := perfKeyNum
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			wtxn := train.NewTransaction(true)
			if err := wtxn.Set(GetKey(nextWrite), val); err != nil {
				b.Fatal(err)
			}
			if _, err := wtxn.Commit(); err != nil {
				b.Fatal(err)
			}
			nextWrite++
		} else {
			if _, err := rtxn.Get(GetKey(i % perfKeyNum)); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkReadIterateSST 迭代器全量顺序扫描 (数据已 flush 到 SST, 走 block 索引/缓存);
func BenchmarkReadIterateSST(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)
	loadData(b, train)
	train.Lsm.Rotate()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(train.Lsm.LevelManger.GetLevelHandler(0).GetTables()) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
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

// BenchmarkReadIterateBigValue 大 value 惰性扫描: 只扫不取值 (Item 携带 ValuePtr, 不拷贝 1MB);
func BenchmarkReadIterateBigValue(b *testing.B) {
	b.ReportAllocs()
	clearDir(perfDataDir)
	train, _, _ := TrainKV.Open(lsm.GetDefaultOpt(perfDataDir))
	defer train.Close()

	// 构造 500MB 到vlog中;
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

	rtxn := train.NewTransaction(false)
	defer rtxn.Discard()

	// NewSkipListIterator().
	// blockIterator.NewChunkedArena(64MB)
	iter := rtxn.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: false})
	defer func() { _ = iter.Close() }()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for iter.Rewind(); iter.Valid(); iter.Next() {
			_ = iter.Item()
			count++
		}
		if count != n {
			b.Fatalf("scan count=%d, want %d", count, n)
		}
	}
}

// BenchmarkReadIterateBigValueEager 大 value 扫描并取值 (调用 Item.Value(), 模拟按需消费);
func BenchmarkReadIterateBigValueEager(b *testing.B) {
	b.ReportAllocs()
	clearDir(perfDataDir)
	train, _, _ := TrainKV.Open(lsm.GetDefaultOpt(perfDataDir))
	defer train.Close()

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

	rtxn := train.NewTransaction(false)
	defer rtxn.Discard()
	iter := rtxn.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: false})
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
		if count != n {
			b.Fatalf("scan count=%d, want %d", count, n)
		}
	}
}
