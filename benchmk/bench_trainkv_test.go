package benchmk

import (
	"fmt"
	"testing"

	"github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/lsm"
)

var trainkv *TrainKV.TrainKV

func init1() {
	fmt.Println("init TrainKV")
	dir := "/tmp/goland_pro_data/bench_1/trainkv_data"
	clearDir(dir)
	opt := lsm.GetDefaultOpt(dir)
	opt.SyncWrites = false
	trainkv, _, _ = TrainKV.Open(opt)
}

// Benchmark_TrainKV_PutValue-4     1000000             10986 ns/op            2311 B/op          6 allocs/op
// Benchmark_TrainKV_GetValue-4     2853574              5319 ns/op            1149 B/op          8 allocs/op
func Benchmark_TrainKV_PutValue(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		trainkv.Update(func(txn *TrainKV.Transaction) error {
			return txn.Set(GetKey(i), GetValue())
		})
	}
}

func Benchmark_TrainKV_GetValue(b *testing.B) {
	for i := 0; i < 500000; i++ {
		trainkv.Update(func(txn *TrainKV.Transaction) error {
			return txn.Set(GetKey(i), GetValue())
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		trainkv.View(func(txn *TrainKV.Transaction) error {
			txn.Get(GetKey(i))
			return nil
		})
	}
}
