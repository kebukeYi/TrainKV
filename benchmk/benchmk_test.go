package benchmk

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
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

func BenchmarkTrainKVTxnSet(b *testing.B) {
	// go test -bench=BenchmarkNormalEntry -benchtime=3s -count=2 -failfast
	// go test -bench=BenchmarkNormalEntry -benchtime=100000x -count=5 -failfast
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkDir)
	train, _, _ := TrainKV.Open(lsm.GetDefaultOpt(benchMarkDir))
	defer train.Close()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		//valSize := 5 + 1 // val: 6B
		valSize := 127 + 1 // val: 12B
		//valSize := 10<<20 + 1 // val: 10.01MB
		//valSize := 64<<20 + 1 // val: 64.01MB
		txn := train.NewTransaction(true)
		entry := model.BuildBigEntry(key, uint64(valSize))
		err := txn.Set(entry.Key, entry.Value)
		assert.Nil(b, err)
		_, err = txn.Commit()
		common.Panic(err)
	}

	txn := train.NewTransaction(false)
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		_, err := txn.Get(key)
		assert.Nil(b, err)

		key = []byte(randStr(18))
		_, err = txn.Get(key)
		assert.Error(b, err)
	}
	txn.Discard()
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
