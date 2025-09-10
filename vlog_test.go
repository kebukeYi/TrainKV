package TrainKV

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

// var vlogTestPath = "/usr/projects_gen_data/goprogendata/trainkvdata/test/vlog"

var vlogTestPath = "F:\\ProjectsData\\golang\\TrainDB\\test\\vlog"

var (
	vlogOpt = &lsm.Options{
		WorkDir:             vlogTestPath,
		MemTableSize:        10 << 10, // 10KB; 64 << 20(64MB)
		NumFlushMemtables:   1,        // 默认：15;
		BlockSize:           2 * 1024, // 4 * 1024
		BloomFalsePositive:  0.01,     // 误差率
		CacheNums:           1 * 1024, // 10240个
		ValueThreshold:      1,        // 1B; 1 << 20(1MB)
		ValueLogMaxEntries:  100,      // 1000000
		ValueLogFileSize:    1 << 29,  // 512MB; 1<<30-1(1GB);
		VerifyValueChecksum: false,    // false

		MaxBatchCount: 1000,
		MaxBatchSize:  10 << 20, // 10 << 20(10MB)

		NumCompactors:       2,       // 4
		BaseLevelSize:       8 << 20, //8MB; 10 << 20(10MB)
		LevelSizeMultiplier: 10,
		TableSizeMultiplier: 2,
		BaseTableSize:       2 << 20, // 2 << 20(2MB)
		NumLevelZeroTables:  5,
		MaxLevelNum:         common.MaxLevelNum,
	}
)

func TestValueLog_Entry(t *testing.T) {
	db, _, callBack := Open(vlogOpt)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		_ = callBack()
	}()
	log := db.vlog
	const val2 = "samplevalb012345678901234567890123"
	e2 := &model.Entry{
		Key:   []byte("samplekeyb"),
		Value: []byte(val2),
		Meta:  common.BitValuePointer,
	}

	// 构建一个批量请求的request
	b := new(Request)
	b.Entries = []*model.Entry{e2}
	// 直接写入vlog中
	log.Write([]*Request{b})
	// 从vlog中使用 value ptr指针中查询写入的分段vlog文件
	buf1, lf1, err1 := log.ReadValueBytes(b.ValPtr[0])
	defer lf1.Lock.RUnlock()
	fmt.Printf("err1: %s\n", err1)
	e1, _ := lf1.DecodeEntry(buf1, b.ValPtr[0].Offset)
	fmt.Printf("key: %s, val:%s \n", e1.Key, e1.Value)
}

func TestVlogBase(t *testing.T) {
	// 清理目录
	clearDir(vlogOpt.WorkDir)
	// 打开DB
	db, _, callBack := Open(vlogOpt)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		_ = callBack()
	}()
	log := db.vlog
	var err error
	// 创建一个简单的kv entry对象
	const val1 = "sampleval012345678901234567890123"
	const val2 = "samplevalb012345678901234567890123"
	require.True(t, len(val1) >= db.Opt.ValueThreshold)

	e1 := &model.Entry{
		Key:   []byte("samplekey"),
		Value: []byte(val1),
		Meta:  common.BitValuePointer,
	}
	e2 := &model.Entry{
		Key:   []byte("samplekeyb"),
		Value: []byte(val2),
		Meta:  common.BitValuePointer,
	}

	// 构建一个批量请求的request
	b := new(Request)
	b.Entries = []*model.Entry{e1, e2}

	// 直接写入vlog中
	log.Write([]*Request{b})
	require.Len(t, b.ValPtr, 2)
	fmt.Printf("Pointer written: %+v %+v\n", b.ValPtr[0], b.ValPtr[1])

	// 从vlog中使用 value ptr指针中查询写入的分段vlog文件
	buf1, lf1, err1 := log.ReadValueBytes(b.ValPtr[0])
	buf2, lf2, err2 := log.ReadValueBytes(b.ValPtr[1])

	require.NoError(t, err1)
	require.NoError(t, err2)
	// 关闭会调的锁
	defer model.RunCallback(log.getUnlockCallBack(lf1))
	defer model.RunCallback(log.getUnlockCallBack(lf2))

	e1, err = lf1.DecodeEntry(buf1, b.ValPtr[0].Offset)
	require.NoError(t, err)

	// 从vlog文件中通过指指针反序列化回 entry对象;
	e2, err = lf1.DecodeEntry(buf2, b.ValPtr[1].Offset)
	require.NoError(t, err)

	// 比较entry对象是否相等
	readEntries := []*model.Entry{e1, e2}
	require.EqualValues(t, []model.Entry{
		{
			Key:    []byte("samplekey"),
			Value:  []byte(val1),
			Meta:   common.BitValuePointer,
			Offset: b.ValPtr[0].Offset,
		},
		{
			Key:    []byte("samplekeyb"),
			Value:  []byte(val2),
			Meta:   common.BitValuePointer,
			Offset: b.ValPtr[1].Offset,
		},
	}, readEntries)
}

func TestValueGC(t *testing.T) {
	clearDir(vlogOpt.WorkDir)
	vlogOpt.ValueLogFileSize = 1 << 20
	db, _, callBack := Open(vlogOpt)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		_ = callBack()
	}()
	sz := 3 << 10
	var kvList []*model.Entry
	for i := 0; i < 40; i++ {
		e := newRandEntry(sz)
		kvList = append(kvList, &model.Entry{
			Key:       e.Key,
			Value:     e.Value,
			Meta:      e.Meta,
			ExpiresAt: e.ExpiresAt,
		})
		require.NoError(t, db.Set(e))
	}
	time.Sleep(2 * time.Second)
	for i := 0; i < 10; i++ {
		entry := model.NewEntry(kvList[i].Key, nil)
		entry.Meta |= common.BitDelete
		require.NoError(t, db.Set(entry))
	}

	// 直接开始GC, 1.pickVlog需要和合并联动; 2.启动 vlog.file 的rewrite();
	// kv.RunValueLogGC(0.9)

	// 指定 1.vlog 文件进行 GC;
	db.vlog.gcReWriteLog(db.vlog.filesMap[0])

	for _, e := range kvList {
		item, err := db.Get(e.Key) // 无 ts
		if err != nil {
			fmt.Printf("err:%s when key is:%s\n", err, e.Key)
		}
		value := getItemValue(t, item)
		if len(value) > vlogOpt.ValueThreshold {
			value = nil
		}
		fmt.Printf("key:%s, val:%s, err:%s\n", e.Key, value, err)
	}
}

func newRandEntry(sz int) *model.Entry {
	v := make([]byte, sz)
	rand.Read(v[:rand.Intn(sz)])
	e := model.BuildEntry()
	e.Value = v
	return e
}

func getItemValue(t *testing.T, item *model.Entry) (val []byte) {
	t.Helper()
	if item.Value == nil {
		return nil
	}
	var v []byte
	v = append(v, item.Value...)
	if v == nil {
		return nil
	}
	return v
}
