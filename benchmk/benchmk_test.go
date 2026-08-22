package benchmk

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/stretchr/testify/assert"
)

var benchMarkDir = "/usr/golanddata/triankv/benchmk2"

func clearDir(dir string) {
	_, err := os.Stat(dir)
	if err == nil {
		if err = os.RemoveAll(dir); err != nil {
			common.Panic(err)
		}
	}
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		_ = fmt.Sprintf("create dir %s failed", dir)
	}
}

// go test -bench=BenchmarkNormalEntry -benchtime=3s -count=2 -failfast
// go test -bench=BenchmarkNormalEntry -benchtime=100000x -count=5 -failfast

// BenchmarkTrainKVTxnSetBigValue 大 value(kv 分离)写路径: value 超过 ValueThreshold(1MB) 走 vlog;
func BenchmarkTrainKVTxnSetBigValue(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkDir)
	train, _, _ := TrainKV.Open(lsm.GetDefaultOpt(benchMarkDir))
	defer train.Close()

	val := make([]byte, 1<<20+1) // 1MB+1 > ValueThreshold(1MB), 复用同一 value 避免基准侧分配干扰;
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		txn := train.NewTransaction(true)
		if err := txn.Set(key, val); err != nil {
			b.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// API 层攒批: 一个事务 10 条 entry 共享一次 fsync, 观察每 op 摊销的刷盘开销;
func BenchmarkTrainKVBatchSet10(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkDir)
	train, _, _ := TrainKV.Open(lsm.GetDefaultOpt(benchMarkDir))
	defer train.Close()

	for i := 0; i < b.N; i++ {
		txn := train.NewTransaction(true)
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("key=%d-%d", i, j)
			val := fmt.Sprintf("val%d-%d", i, j)
			if err := txn.Set([]byte(key), []byte(val)); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func randStr(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func BenchmarkWriteRequest(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkDir)
	traindb, _, _ := TrainKV.Open(lsm.GetDefaultOpt(benchMarkDir))
	defer traindb.Close()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key=%d", i)
		val := fmt.Sprintf("val%d", i)
		//val := make([]byte, 10<<20+1)
		e := model.NewEntry([]byte(key), []byte(val))
		e.Key = model.KeyWithTs(e.Key, 0)
		request := TrainKV.BuildRequest([]*model.Entry{e})
		if err := traindb.WriteRequest([]*model.Request{request}); err != nil {
			assert.Nil(b, err)
		} else {
			err := request.Wait()
			assert.Nil(b, err)
		}
	}

	txn := traindb.NewTransaction(false)
	defer txn.Discard()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		_, err := txn.Get(key)
		assert.Nil(b, err)

		key = []byte(randStr(18))
		_, err = txn.Get(key)
		assert.Error(b, err)
	}
}
