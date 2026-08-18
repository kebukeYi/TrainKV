package benchmk

// 全方位读写流程性能基准;
//
// 读流程: memtable 命中(顺序/随机)、未命中、vlog 大 value、迭代器全量扫描;
// 写流程: 串行单条(128B/4KB)、批量 10 条、并发、大 value(1MB, kv 分离)、直写 API;
// 混合:   9:1 读:写;
//
// 注意: 当前工作区 flush→SST 路径存在预存数据问题(flush 后约 1.4% key 丢失、
// 连续轮转时偶发 panic), 故读流程基准的数据全部驻留 memtable(100K*512B=50MB<64MB),
// 不触发轮转; SST 读路径暂无法可靠测出性能数字, 详见测试报告。

import (
	"math/rand"
	"testing"
	"time"

	"github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/lsm"
)

var perfDataDir = "/usr/golanddata/triankv/perf2"

const (
	perfKeyNum = 100000 // 读数据集规模: 100K * 512B ≈ 50MB, 小于 64MB memtable, 不触发轮转;
	perfBatch  = 500    // 装载时的批大小;
)

// loadMemData 装载 perfKeyNum 个 key(512B value), 数据全部驻留 memtable;
func loadMemData(b *testing.B, train *TrainKV.TrainKV) {
	txn := train.NewTransaction(true)
	for i := 0; i < perfKeyNum; i++ {
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

func openPerfDB(b *testing.B) *TrainKV.TrainKV {
	clearDir(perfDataDir)
	train, _, _ := TrainKV.Open(lsm.GetDefaultOpt(perfDataDir))
	b.Cleanup(func() { _ = train.Close() })
	return train
}

// ---------------- 写流程 ----------------

// BenchmarkWriteTxnSet 串行事务单条提交, 128B value, 纯写(无读阶段);
func BenchmarkWriteTxnSet(b *testing.B) {
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

// BenchmarkWriteTxnSet4K 串行事务单条提交, 4KB value(仍低于 1MB 阈值, 走 LSM 直写);
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

// ---------------- 读流程 ----------------

// BenchmarkReadGetHitSeq 顺序命中, 数据在 memtable;
func BenchmarkReadGetHitSeq(b *testing.B) {
	b.ReportAllocs()
	train := openPerfDB(b)
	loadMemData(b, train)
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
	b.ReportAllocs()
	train := openPerfDB(b)
	loadMemData(b, train)
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
	loadMemData(b, train)
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
	loadMemData(b, train)
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
	loadMemData(b, train)
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
	loadMemData(b, train)
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
