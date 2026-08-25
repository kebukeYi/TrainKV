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
	defaultOpt.SyncWrites = false
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
// wal_no_sync ↓
// 342991	         3361 ns/op	              104 B/op	       4 allocs/op
// wal_sync ↓
// 0002793           4030367 ns/op            107 B/op          4 allocs/op
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
// wal_no_sync ↓
// 3300876              3789 ns/op             803 B/op          7 allocs/op
// 00243478	            4287 ns/op	     	   522 B/op	         6 allocs/op
// wal_sync ↓
// 0007284           1688478 ns/op            9567 B/op          7 allocs/op
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

// BenchmarkWriteTxnSet 串行事务单条提交, 128B value, 纯写(无读阶段);
// wal_no_sync ↓
// 3693842              3225 ns/op             630 B/op          4 allocs/op
// wal_sync ↓
// 0002793           4030367 ns/op             107 B/op          4 allocs/op
func BenchmarkWriteTxnSet512B(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)

	val := make([]byte, 512)
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
// wal_no_sync ↓
// 3300876              3789 ns/op             803 B/op          7 allocs/op
// wal_sync ↓
// 0007284           1688478 ns/op            9567 B/op          7 allocs/op
func BenchmarkWriteTxnSet512BParallel(b *testing.B) {
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
			if err := txn.Set(key, make([]byte, 512)); err != nil {
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
// 1000000             39911 ns/op           19318 B/op         11 allocs/op
// wal_sync ↓
// 0001322           8366210 ns/op             113 B/op          4 allocs/op
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
// wal_no_sync ↓
// 1000000             49941 ns/op           19316 B/op         11 allocs/op
// wal_sync ↓
// 0003656           4550411 ns/op             171 B/op          4 allocs/op
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

// wal_no_sync ↓
// 8118           4023453 ns/op             367 B/op          4 allocs/op
// wal_sync ↓
// 0444          25728717 ns/op            4859 B/op          4 allocs/op
func BenchmarkWriteTxnSet2MB(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)
	val := make([]byte, 2<<20)
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

// wal_no_sync ↓
// 6261           3729218 ns/op            2461 B/op          4 allocs/op
// wal_sync ↓
// 0922          11891591 ns/op           16176 B/op          4 allocs/op
func BenchmarkWriteTxnSet2MBParallel(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)
	val := make([]byte, 2<<20)
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
// BenchmarkReadGetHitSeq-4   	  963973	      1224 ns/op	     120 B/op	       2 allocs/op
// BenchmarkReadGetHitSeq-4   	 1669935	       705.2 ns/op	     120 B/op	       2 allocs/op
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
// BenchmarkReadGetHitRandom-4   	  537807	      2440 ns/op	     120 B/op	       2 allocs/op
// BenchmarkReadGetHitRandom-4   	  685623	      1579 ns/op	     120 B/op	       2 allocs/op
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
// BenchmarkReadGetMiss-4   	 1000000	      1136 ns/op	     120 B/op	       2 allocs/op
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
// BenchmarkReadGetBigValue-4   	       1	1170131404 ns/op	 1056952 B/op	       6 allocs/op
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
// BenchmarkReadIterate-4   	      48	  23968464 ns/op	       0 B/op	       0 allocs/op
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

// ---------------- 读写混合 ----------------

// BenchmarkMixedRead90Write10 9:1 读写混合: 90% 读已有 key, 10% 提交新 key;
// 13563            888557 ns/op             132 B/op          2 allocs/op
func BenchmarkMixedRead90Write10(b *testing.B) {
	train := openPerfDB(b)
	loadData(b, train)
	b.ReportAllocs()
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
// 410          28593237 ns/op         4569425 B/op     128316 allocs/op
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
// BenchmarkReadIterateBigValue-4   	       1	1550893573 ns/op	      48 B/op	       2 allocs/op
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
// BenchmarkReadIterateBigValueEager-4   	       1	1419229138 ns/op	528457056 B/op	    1510 allocs/op
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
