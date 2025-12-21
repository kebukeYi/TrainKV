package benchmk

import (
	"fmt"
	"github.com/kebukeYi/TrainKV"
	"github.com/kebukeYi/TrainKV/lsm"
	"testing"
)

var triandb *TrainKV.TrainKV

func initTrainDB() {
	fmt.Println("init TrainDB")
	//dir := "F:\\ProjectsData\\golang\\TrainDB\\benchmk"
	dir := "/usr/golanddata/triandb/benchmk1"
	clearDir(dir)
	trianDB, err, _ := TrainKV.Open(lsm.GetDefaultOpt(dir))
	if err != nil {
		panic(err)
	}
	triandb = trianDB
}

func Benchmark_PutValue_TrainDB(b *testing.B) {
	initTrainDB()
	defer triandb.Close()

	txn := triandb.NewTransaction(true)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := txn.Set(GetKey(i), GetValue())
		if err != nil {
			panic(err)
			return
		}
	}
	_, err := txn.Commit()
	if err != nil {
		panic(err)
		return
	}
}

func initTrainDBData() {
	txn := triandb.NewTransaction(false)
	for i := 0; i < 500000; i++ {
		if i%5000 == 0 {
			_, err := txn.Commit()
			if err != nil {
				panic(err)
				return
			}
			txn = triandb.NewTransaction(false)
		}
		err := txn.Set(GetKey(i), GetValue())
		if err != nil {
			panic(err)
			return
		}
	}
}

func Benchmark_GetValue_TrainDB(b *testing.B) {
	initTrainDB()
	initTrainDBData()
	defer triandb.Close()

	b.ResetTimer()
	b.ReportAllocs()
	txn := triandb.NewTransaction(false)
	for i := 0; i < b.N; i++ {
		_, err := txn.Get(GetKey(i))
		if err != nil {
			panic(err)
		}
	}
}
