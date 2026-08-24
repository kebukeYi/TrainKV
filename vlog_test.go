package TrainKV

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	_, err := log.Write([]*model.Request{b})
	assert.Nil(t, err)
	// 从vlog中使用 value ptr指针中查询写入的分段vlog文件
	buf1, lf1, err1 := log.ReadValueBytes(&b.ValPtr[0])
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
	_, err = log.Write([]*model.Request{b})
	require.NoError(t, err)

	require.Len(t, b.ValPtr, 2)
	fmt.Printf("Pointer written: %+v %+v\n", b.ValPtr[0], b.ValPtr[1])

	// 从vlog中使用 value ptr指针中查询写入的分段vlog文件
	buf1, lf1, err1 := log.ReadValueBytes(&b.ValPtr[0])
	buf2, lf2, err2 := log.ReadValueBytes(&b.ValPtr[1])

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
			continue // 已删除的 key 无 item, 跳过取值;
		}
		value := getItemValue(t, item)
		if int64(len(value)) > vlogOpt.ValueThreshold {
			value = nil
		}
		fmt.Printf("key:%s, val:%s, err:%s\n", e.Key, value, err)
	}
	txn.Discard()
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

func TestVlogWriteRotationErrorPropagated(t *testing.T) {
	dir := t.TempDir()
	opt := lsm.GetDefaultOpt(dir)
	opt.ValueLogFileSize = 100 // 极小,强制写入中轮转;
	opt.ValueThreshold = 10    // 使 200B 的 value 落入 vlog (否则走 LSM 路径, 不会触发轮转);
	opt.ValueLogMaxEntries = 1000
	db, _, _ := Open(opt)
	defer db.Close()
	// 预建占位目录 00002.vlog文件, 轮转时 createVlogFile → os.OpenFile(2) 返回失败
	require.NoError(t, os.Mkdir(filepath.Join(dir, "00002.vlog"), 0o755))
	txn := db.NewTransaction(true)
	require.NoError(t, txn.Set([]byte("k1"), bytes.Repeat([]byte("v"), 200)))
	// 触发写入 → vlog 文件 00001 超 100B → toWrite → DoneWriting + createVlogFile(2) 失败
	_, err := txn.Commit()
	// 期望:err != nil;实际(现状):err == nil,数据已"成功"提交, 但 vlog 无此数据;
	if err == nil {
		t.Fatalf("BUG 复现: 提交成功但 vlog 数据丢失")
	}
}

// TestVlogGCRewriteInversion 回归测试 (方案二: GC 重写直落 L0):
// vlogGC (gcReWriteLog) 重写的旧版本直接构建成 SST 进 L0, 不再进入 memtable ——
// 即使"旧版本仍被 LSM 引用"且"新版本已在 imm/L0", 也不会产生存储层版本顺序反转;
// 普通读 (startTs == ts2) 必须读到新版本, 旧快照 (startTs == ts1) 必须读到旧版本
// (GC 重写正是为了维持旧引用的有效性)。
func TestVlogGCRewriteInversion(t *testing.T) {
	removeAll(vlogTestPath)
	opt := lsm.GetDefaultOpt(vlogTestPath)
	opt.ValueThreshold = 10    // 小阈值, 让 value 落入 vlog;
	opt.ValueLogFileSize = 100 // 极小, 强制第一个版本写入时轮转: 旧版本在 F1, 新版本在 F2 (GC 只处理非当前文件);
	opt.NumCompactors = 0      // 关闭 compaction, 防止旧版本被提前清除, 保证反转场景可复现;
	db, _, callBack := Open(opt)
	defer func() {
		require.NoError(t, db.Close())
		_ = callBack()
	}()

	rawKey := []byte("gcInversionKey")
	oldVal := bytes.Repeat([]byte("v1"), 200) // ≥ ValueThreshold, 进 vlog;
	newVal := bytes.Repeat([]byte("v2"), 200)

	// ① 写入旧版本 K@ts1, 强制轮转并等待 flush 到 L0 (旧版本被 LSM 引用, GC 才会重写它);
	txn := db.NewTransaction(true)
	require.NoError(t, txn.Set(rawKey, oldVal))
	_, err := txn.Commit()
	require.NoError(t, err)
	db.Lsm.Rotate()
	deadline := time.Now().Add(5 * time.Second)
	for len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables()) < 1 {
		if time.Now().After(deadline) {
			t.Fatal("flush to L0 timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// ② 旧快照读事务 (startTs == ts1), 稍后验证 GC 重写后的旧版本仍可读;
	oldTxn := db.NewTransaction(false)
	defer oldTxn.Discard()

	// ③ 写入新版本 K@ts2 (进 memtable);
	txn = db.NewTransaction(true)
	require.NoError(t, txn.Set(rawKey, newVal))
	_, err = txn.Commit()
	require.NoError(t, err)

	// ④ 再次轮转: K@ts2 进入 imm, 等待其异步 flush 到 L0 (保证后续 L0 表数断言确定);
	db.Lsm.Rotate()
	deadline = time.Now().Add(5 * time.Second)
	for len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables()) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("imm flush to L0 timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// ⑤ vlogGC 重写 vlog 文件 1 中的旧版本 K@ts1 → 直落 L0 构建新表, 不碰 memtable;
	l0TablesBefore := len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables())
	require.NoError(t, db.vlog.gcReWriteLog(db.vlog.filesMap[1]))

	// ⑤-1 结构性验证: GC 重写没有污染 memtable, 而是新增了一张 L0 表;
	require.True(t, db.Lsm.GetSkipListFromMemTable().Empty(), "GC 重写不得写入 memtable")
	require.Equal(t, l0TablesBefore+1, len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables()),
		"GC 重写应新增一张 L0 表")

	// ⑥ 新读事务 (startTs == ts2): 必须读到新版本;
	newTxn := db.NewTransaction(false)
	defer newTxn.Discard()
	item, err := newTxn.Get(rawKey)
	require.NoError(t, err)
	require.Equal(t, newVal, getItemValue(t, item), "必须返回新版本")

	// ⑦ 旧快照读事务 (startTs == ts1): 仍应读到旧版本 (值已迁移到新 vlog 文件, 引用依然有效);
	oldItem, err := oldTxn.Get(rawKey)
	require.NoError(t, err)
	require.Equal(t, oldVal, getItemValue(t, oldItem), "旧快照应读到旧版本")
}

// TestVlogGCRewriteInversionReopen 验证 GC 直落 L0 的表在重启后依然可用:
// 表已写入 manifest (fsync), ValuePtr 指向的新 vlog 文件在重启后正常加载;
func TestVlogGCRewriteInversionReopen(t *testing.T) {
	removeAll(vlogTestPath)
	opt := lsm.GetDefaultOpt(vlogTestPath)
	opt.ValueThreshold = 10
	opt.ValueLogFileSize = 100
	opt.NumCompactors = 0

	rawKey := []byte("gcReopenKey")
	oldVal := bytes.Repeat([]byte("v1"), 200)
	newVal := bytes.Repeat([]byte("v2"), 200)

	// 写入旧版本并 flush 到 L0, 再写入新版本;
	setup := func(db *TrainKV) {
		txn := db.NewTransaction(true)
		require.NoError(t, txn.Set(rawKey, oldVal))
		_, err := txn.Commit()
		require.NoError(t, err)
		db.Lsm.Rotate()
		deadline := time.Now().Add(5 * time.Second)
		for len(db.Lsm.LevelManger.GetLevelHandler(0).GetTables()) < 1 {
			if time.Now().After(deadline) {
				t.Fatal("flush to L0 timeout")
			}
			time.Sleep(10 * time.Millisecond)
		}
		txn = db.NewTransaction(true)
		require.NoError(t, txn.Set(rawKey, newVal))
		_, err = txn.Commit()
		require.NoError(t, err)
	}

	// 第一次打开: 写两个版本 → GC 重写旧版本直落 L0;
	db, _, callBack := Open(opt)
	setup(db)
	require.NoError(t, db.vlog.gcReWriteLog(db.vlog.filesMap[1]))
	require.NoError(t, db.Close())
	_ = callBack()

	// 第二次打开: GC 表已持久化 (manifest), 值已迁移到新 vlog 文件;
	db2, _, callBack2 := Open(opt)
	defer func() {
		require.NoError(t, db2.Close())
		_ = callBack2()
	}()
	txn := db2.NewTransaction(false)
	defer txn.Discard()
	item, err := txn.Get(rawKey)
	require.NoError(t, err)
	require.Equal(t, newVal, getItemValue(t, item), "重启后应读到新版本")
}
