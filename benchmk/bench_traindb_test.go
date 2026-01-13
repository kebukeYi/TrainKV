package benchmk

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"testing"
)

var triandb *TrainKV.TrainKV

func initTrainDB() {
	fmt.Println("init TrainDB")
	dir := "/usr/golanddata/triandb/benchmk1"
	clearDir(dir)
	trianDB, err, _ := TrainKV.Open(lsm.GetDefaultOpt(dir))
	if err != nil {
		panic(err)
	}
	triandb = trianDB
}

func initTrainDBData() {
	txn := triandb.NewTransaction(true)
	for i := 1; i <= 500000; i++ {
		err := txn.Set(GetKey(i), GetValue())
		if err != nil {
			panic(err)
			return
		}
		if i%500 == 0 {
			_, err = txn.Commit()
			if err != nil {
				panic(err)
				return
			}
			txn = triandb.NewTransaction(true)
		}
	}
}

func Benchmark_PutValue_TrainDB(b *testing.B) {
	initTrainDB()
	defer triandb.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn := triandb.NewTransaction(true)
		err := txn.Set(GetKey(i), GetValue())
		if err != nil {
			panic(err)
			return
		}
		_, err = txn.Commit()
		if err != nil {
			panic(err)
			return
		}
	}
}

func Benchmark_GetValue_TrainDB(b *testing.B) {
	initTrainDB()
	initTrainDBData()
	fmt.Println("init TrainDBData over.")
	defer triandb.Close()

	b.ResetTimer()
	b.ReportAllocs()
	txn := triandb.NewTransaction(false)
	b.N = 500000
	for i := 1; i <= b.N; i++ {
		_, err := txn.Get(GetKey(i))
		if err != nil {
			fmt.Printf("key:%s \n", GetKey(i))
			panic(err)
		}
	}
	txn.Discard()
}
