package src

import (
	"fmt"
	"os"
	"testing"
	"trainKv/common"
	"trainKv/lsm"
	"trainKv/model"
)

var dbTestOpt = &lsm.Options{
	WorkDir:            "/usr/projects_gen_data/goprogendata/trainkvdata/test/vlog",
	MemTableSize:       1 << 10,
	SSTableMaxSz:       1 << 10,
	ValueLogFileSize:   1 << 11,
	ValueThreshold:     1,
	MaxBatchCount:      10,
	MaxBatchSize:       1 << 20,
	ValueLogMaxEntries: 100,
	BloomFalsePositive: 0.1,
	CacheNums:          10240,
	BlockSize:          200,
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

func TestAPI(t *testing.T) {
	clearDir()
	db, _ := Open(dbTestOpt)
	defer func() { _ = db.Close() }()
	putStart := 0
	putEnd := 60
	putStart1 := 70
	putEnd1 := 90
	delStart := 0
	delEnd := 40
	fmt.Println("========================put(0-60)==================================")
	// 写入 0-60
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		e := model.NewEntry([]byte(key), []byte(val))
		e.ExpiresAt = uint64(i)
		if i == 60 {
			e.ExpiresAt = 60000000000
		} else if i == 6 {
			e.ExpiresAt = 66666666666
		} else if i == 2 {
			e.ExpiresAt = 2222222222
		} else if i == 20 {
			e.ExpiresAt = 2000000000
		}
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("========================get(0-60)==================================")
	// 读取
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		// 查询
		if entry, err := db.Get([]byte(key)); err != nil {
			//t.Fatal(err)
			fmt.Printf("db.Get key=%s, err: %v \n", key, err)
		} else {
			//t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
			fmt.Printf("db.Get key=%s, value=%s, meta:%d, expiresAt=%d \n",
				entry.Key, entry.Value, entry.Meta, entry.ExpiresAt)
		}
	}

	fmt.Println("========================del(0-40)==================================")
	// 写入删除 0-40 剩余 41-60
	for i := delStart; i <= delEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		if err := db.Del([]byte(key), uint64(i)); err != nil {
			t.Fatal(err)
		}

		//if entry, err := db.Get([]byte(key)); err != nil {
		//	//t.Fatal(err)
		//	fmt.Printf("err db.Get key=%s, err: %v \n", key, err)
		//} else {
		//	//t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		//	fmt.Printf("db.Get key=%s, value=%s, expiresAt=%d \n", entry.Key, entry.Value, entry.ExpiresAt)
		//}
	}

	fmt.Println("========================put(70-90)=================================")
	// 写入 70-90
	for i := putStart1; i <= putEnd1; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		e := model.NewEntry([]byte(key), []byte(val))
		e.ExpiresAt = uint64(i)
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("========================get(0-90)==================================")
	// 读取
	for i := putStart; i <= putEnd1; i++ {
		key := fmt.Sprintf("key%d", i)
		if entry, err := db.Get([]byte(key)); err != nil {
			//t.Fatal(err)
			fmt.Printf("err db.Get key=%s, err: %v \n", key, err)
		} else {
			//t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
			fmt.Printf("db.Get key=%s, value=%s, meta:%d, expiresAt=%d \n",
				entry.Key, entry.Value, entry.Meta, entry.ExpiresAt)
		}
	}

	fmt.Println("=========================iter(41-60 70-90)===========================")
	// 迭代器 正确的应该是 41-60 70-90
	iter := db.NewDBIterator(&model.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	iter.Rewind()
	for iter.Valid() {
		it := iter.Item()
		//t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", utils.ParseKey(it.Entry().Key), it.Entry().Value, it.Entry().ExpiresAt)
		fmt.Printf("db.Iterator key=%s, value=%s,meta:%d, expiresAt=%d \n",
			model.ParseKey(it.Item.Key), it.Item.Value, it.Item.Meta, it.Item.ExpiresAt)
		iter.Next()
	}
}

func TestMultiPutDelGet(t *testing.T) {
	clearDir()
	db, _ := Open(dbTestOpt)
	defer func() { _ = db.Close() }()
	putStart := 0
	putEnd := 10
	fmt.Println("========================put(0-60)==================================")
	// 写入 0-60
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("putVal%d", i)
		e := model.NewEntry([]byte(key), []byte(val))
		e.ExpiresAt = uint64(i)
		if i == 60 {
			e.ExpiresAt = 60000000000
		} else if i == 6 {
			e.ExpiresAt = 66666666666
		} else if i == 2 {
			e.ExpiresAt = 2222222222
		} else if i == 20 {
			e.ExpiresAt = 2000000000
		}
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("========================update1(0-60)==================================")
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("update1Val%d", i)
		e := model.NewEntry([]byte(key), []byte(val))
		e.ExpiresAt = uint64(i)
		if i == 60 {
			e.ExpiresAt = 60000000000
		} else if i == 6 {
			e.ExpiresAt = 66666666666
		} else if i == 2 {
			e.ExpiresAt = 2222222222
		} else if i == 20 {
			e.ExpiresAt = 2000000000
		}
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("========================update2(0-60)==================================")
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("update2Val%d", i)
		e := model.NewEntry([]byte(key), []byte(val))
		e.ExpiresAt = uint64(i)
		if i == 60 {
			e.ExpiresAt = 60000000000
		} else if i == 6 {
			e.ExpiresAt = 66666666666
		} else if i == 2 {
			e.ExpiresAt = 2222222222
		} else if i == 20 {
			e.ExpiresAt = 2000000000
		}
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReStart(t *testing.T) {
	db, _ := Open(dbTestOpt)
	defer func() { _ = db.Close() }()
	putStart := 0
	putEnd := 90
	// 读取
	fmt.Println("=============db.get=========================================")
	for i := putStart; i <= putEnd; i++ {
		key := fmt.Sprintf("key%d", i)
		// 查询
		if entry, err := db.Get([]byte(key)); err != nil {
			//t.Fatal(err)
			fmt.Printf("err db.Get key=%s, err: %v \n", key, err)
		} else {
			//t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
			fmt.Printf("db.Get key=%s, value=%s, expiresAt=%d \n", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}
	fmt.Println("=============db.Iterator=========================================")
	// 迭代器
	iter := db.NewDBIterator(&model.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	iter.Rewind()
	for iter.Valid() {
		it := iter.Item()
		//t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", utils.ParseKey(it.Entry().Key), it.Entry().Value, it.Entry().ExpiresAt)
		//fmt.Printf("db.NewIterator key=%s, value=%s, expiresAt=%d", utils.ParseKey(it.Entry().Key), it.Entry().Value, it.Entry().ExpiresAt)
		fmt.Printf("db.Iterator key=%s, len(value)=%d, Meta=%d, expiresAt=%d \n",
			it.Item.Key, len(it.Item.Value), it.Item.Meta, it.Item.ExpiresAt)
		iter.Next()
	}
	fmt.Println("======================over====================================")
}
