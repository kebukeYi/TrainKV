package TrainKV

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

var vlogTestPath = "/usr/golanddata/trainkv/vlog"

func TestValueLog_Entry(t *testing.T) {
	// 清理目录
	removeAll(vlogTestPath)
	opt := lsm.GetDefaultOpt(vlogTestPath)
	opt.ValueThreshold = 10
	db, _, callBack := Open(opt)
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
	b := new(model.Request)
	b.Entries = []*model.Entry{e2}
	// 直接写入vlog中
	err := log.Write([]*model.Request{b})
	assert.Nil(t, err)
	// 从vlog中使用 value ptr指针中查询写入的分段vlog文件
	buf1, lf1, err1 := log.ReadValueBytes(b.ValPtr[0])
	defer lf1.Lock.RUnlock()
	fmt.Printf("err1: %s\n", err1)
	e1, _ := lf1.DecodeEntry(buf1, b.ValPtr[0].Offset)
	fmt.Printf("key: %s, val:%s \n", e1.Key, e1.Value)
}

func TestVlogBase(t *testing.T) {
	// 清理目录
	removeAll(vlogTestPath)
	opt := lsm.GetDefaultOpt(vlogTestPath)
	opt.ValueThreshold = 10
	// 打开DB
	db, _, callBack := Open(opt)
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
	require.True(t, int64(len(val1)) >= db.Opt.ValueThreshold)

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
	b := new(model.Request)
	b.Entries = []*model.Entry{e1, e2}

	// 直接写入vlog中
	err = log.Write([]*model.Request{b})
	require.NoError(t, err)

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
	require.EqualValues(t, []*model.Entry{
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
	removeAll(vlogTestPath)
	vlogOpt := lsm.GetDefaultOpt(vlogTestPath)
	vlogOpt.ValueLogFileSize = 10000
	vlogOpt.ValueThreshold = 10
	db, _, callBack := Open(vlogOpt)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		_ = callBack()
	}()
	sz := 3 << 10
	var kvList []*model.Entry
	// 先写入 key_0 key_39
	for i := 0; i < 40; i++ {
		sprintf := fmt.Sprintf("key_%d", i)
		e := &model.Entry{
			Key:   []byte(sprintf),
			Value: make([]byte, sz),
		}
		kvList = append(kvList, &model.Entry{
			Key:       e.Key,
			Value:     e.Value,
			Meta:      e.Meta,
			ExpiresAt: e.ExpiresAt,
		})
		txn := db.NewTransaction(true)
		require.NoError(t, txn.SetEntry(e))
		_, err := txn.Commit()
		require.NoError(t, err)
	}
	time.Sleep(2 * time.Second)

	// 删除 key_0 key_9
	for i := 0; i < 10; i++ {
		entry := model.NewEntry(kvList[i].Key, nil)
		entry.Meta = common.BitDelete
		txn := db.NewTransaction(true)
		require.NoError(t, txn.SetEntry(entry))
		_, err := txn.Commit()
		require.NoError(t, err)
	}

	// 对vlog中的每个kv进行判断: LSM中有的话, 说明有效数据(哪怕有更高版本的删除标记); 就再重新写到新文件中,否则就丢弃掉;
	// vlogGC严重依赖sst的通知;
	// 1.pickVlog需要和合并联动; 2.启动 vlog.file 的rewrite();
	// kv.RunValueLogGC(0.9)

	// 指定 1.vlog 文件进行 GC;
	vLogFile := db.vlog.filesMap[1]
	err := db.vlog.gcReWriteLog(vLogFile)
	require.NoError(t, err)

	txn := db.NewTransaction(false)
	for _, e := range kvList {
		item, err := txn.Get(e.Key) // 无 ts
		if err != nil {
			fmt.Printf("err:%s when key is:%s\n", err, e.Key)
		}
		value := getItemValue(t, item)
		if int64(len(value)) > vlogOpt.ValueThreshold {
			value = nil
		}
		fmt.Printf("key:%s, val:%s, err:%s\n", e.Key, value, err)
	}
	txn.Discard()
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
