package TrainKV

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
	"os"
	"testing"
)

var dbTestPath = "/usr/projects_gen_data/goprogendata/trainkvdata/test/db"

var dbTestOpt = &lsm.Options{
	WorkDir:             dbTestPath,
	MemTableSize:        1 << 10,  // 1KB; 64 << 20(64MB)
	NumFlushMemtables:   10,       // 默认:15;
	SSTableMaxSz:        1 << 10,  // 同上
	BlockSize:           3 * 1024, // 4 * 1024;
	BloomFalsePositive:  0.01,     // 误差率;
	CacheNums:           1 * 1024, // 10240个
	ValueThreshold:      1,        // 1B; 1 << 20(1MB)
	ValueLogMaxEntries:  100,      // 1000000
	ValueLogFileSize:    1 << 29,  // 512MB; 1<<30-1(1GB);
	VerifyValueChecksum: false,    // false

	MaxBatchCount: 100,
	MaxBatchSize:  10 << 20, // 10 << 20(10MB)

	NumCompactors:       2,       // 4
	BaseLevelSize:       8 << 20, //8MB; 10 << 20(10MB)
	LevelSizeMultiplier: 10,
	TableSizeMultiplier: 2,
	BaseTableSize:       2 << 20, // 2 << 20(2MB)
	NumLevelZeroTables:  5,
	MaxLevelNum:         common.MaxLevelNum,
}

func clearDir() {
	_, err := os.Stat(dbTestOpt.WorkDir)
	if err == nil {
		if err = os.RemoveAll(dbTestOpt.WorkDir); err != nil {
			common.Panic(err)
		}
	}
	err = os.Mkdir(dbTestOpt.WorkDir, os.ModePerm)
	if err != nil {
		_ = fmt.Sprintf("create dir %s failed", dbTestOpt.WorkDir)
	}
}

func TestOpenTrainDBNoOpt(t *testing.T) {
	//db, err, callBack := Open(nil)
	//db.Close()
	//err = callBack()
	//if err != nil {
	//	return
	//}
	//fmt.Printf("open db err:%v \n", err)

	db, err, callBack := Open(dbTestOpt)
	defer func() {
		db.Close()
		err = callBack()
		if err != nil {
			return
		}
	}()
	fmt.Printf("err:%v \n", err)
}

func TestAPI(t *testing.T) {
	clearDir()
	db, _, callBack := Open(dbTestOpt)
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()
	putStart := 0
	putEnd := 60
	putStart1 := 70
	putEnd1 := 90
	delStart := 0
	delEnd := 40
	fmt.Println("========================put1(0-60)==================================")
	// 写入 0-60 version=1
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		e := model.NewEntry([]byte(key), []byte(val))
		e.Version = 1
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("========================get1(0-60)==================================")
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		if entry, err := db.Get([]byte(key)); err != nil {
			fmt.Printf("err: %v; db.Get key=%s ,version:%d \n", err, key, entry.Version)
		} else {
			//fmt.Printf("db.Get key=%s, value=%s, meta:%d,version=%d \n", model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
		}
	}

	fmt.Println("========================del(0-40)==================================")
	// 写入删除 0-40,version:2 ;剩余 41-60;
	for i := delStart; i <= delEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		if err := db.Del([]byte(key), int64(2)); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("========================put2(70-90)=================================")
	// 写入 70-90,version:3;
	for i := putStart1; i <= putEnd1; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		e := model.NewEntry([]byte(key), []byte(val))
		e.Version = 3
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("========================get2(0-90)==================================")
	for i := putStart; i <= putEnd1; i++ {
		key := fmt.Sprintf("key%d", i)
		if entry, err := db.Get([]byte(key)); err != nil {
			fmt.Printf("err: %v; db.Get key=%s \n", err, key)
		} else {
			fmt.Printf("db.Get key=%s, value=%s, meta:%d, version=%d \n",
				model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
		}
	}

	fmt.Println("=========================iter(41-60 70-90)===========================")
	iter := db.NewDBIterator(&model.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	iter.Rewind()
	for iter.Valid() {
		it := iter.Item()
		if it.Item.Version != -1 {
			fmt.Printf("db.Iterator key=%s, value=%s, meta:%d, version=%d \n",
				model.ParseKey(it.Item.Key), it.Item.Value, it.Item.Meta, it.Item.Version)
		}
		iter.Next()
	}
}

func TestReStart(t *testing.T) {
	db, _, callBack := Open(dbTestOpt)
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()
	putStart := 0
	putEnd := 90
	// 读取
	fmt.Println("=============db.get=========================================")
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		if entry, err := db.Get([]byte(key)); err != nil {
			fmt.Printf("err db.Get key=%s, version:%d, err: %v \n", key, entry.Version, err)
		} else {
			fmt.Printf("ok  db.Get key=%s, value=%s,meta:%d, version=%d \n",
				model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
		}
	}
	fmt.Println("=============db.Iterator=========================================")
	iter := db.NewDBIterator(&model.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	iter.Rewind()
	for iter.Valid() {
		it := iter.Item()
		if it.Item.Version != -1 {
			fmt.Printf("db.Iterator key=%s, value=%s, Meta=%d, versioin=%d \n",
				model.ParseKey(it.Item.Key), it.Item.Value, it.Item.Meta, it.Item.Version)
		}
		iter.Next()
	}
	fmt.Println("======================over====================================")
}
