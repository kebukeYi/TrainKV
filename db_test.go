package TrainKV

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/lsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

var dbTestPath = "/usr/golanddata/trainkv/db"

func TestReOpen(t *testing.T) {
	dir := dbTestPath
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, _ := Open(opt)
	require.NoError(t, err)
	require.NotNil(t, db)

	txn := db.NewTransaction(true)
	// 创建多个条目进行批量操作测试
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("batch-key-%d", i))
		value := []byte(fmt.Sprintf("batch-value-%d", i))
		err = txn.Set(key, value)
		assert.NoError(t, err)
	}
	_, err = txn.Commit()
	assert.NoError(t, err)
	err = db.Close()
	if err != nil {
		panic(err)
		return
	}

	reDB, err, _ := Open(opt)
	defer reDB.Close()
	require.NoError(t, err)
	require.NotNil(t, db)
	txn1 := reDB.NewTransaction(true)
	defer txn1.Discard()
	iter := txn1.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: false})
	defer func() {
		err := iter.Close()
		assert.NoError(t, err)
	}()

	count := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		assert.NotNil(t, item.Item)
		assert.NotNil(t, item.Item.Key)
		assert.NotNil(t, item.Item.Value)
		count++
	}

	assert.Equal(t, 10, count)
}

func TestDBOpenAndClose(t *testing.T) {
	dir := dbTestPath
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	require.NotNil(t, db)

	// 测试重复打开同一个目录应该失败
	db2, err2, _ := Open(opt)
	assert.Error(t, err2)
	assert.Equal(t, common.ErrLockDB, err2)
	assert.Nil(t, db2)

	// 关闭数据库
	err = db.Close()
	assert.NoError(t, err)
	_ = callBack()

	// 现在应该可以重新打开
	db3, err3, callBack3 := Open(opt)
	require.NoError(t, err3)
	require.NotNil(t, db3)
	err = db3.Close()
	assert.NoError(t, err)
	_ = callBack3()
}

func TestDBBasicOperations(t *testing.T) {
	dir := dbTestPath
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()

	// 测试基本的 Set/get 操作
	key := []byte("test-key")
	value := []byte("test-value")

	// 使用事务进行设置
	txn := db.NewTransaction(true)
	err = txn.Set(key, value)
	assert.NoError(t, err)

	commitTs, err := txn.Commit()
	assert.NoError(t, err)
	assert.True(t, commitTs > 0)

	// 使用事务进行读取
	txn2 := db.NewTransaction(false)
	defer txn2.Discard()

	entry, err := txn2.Get(key)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, value, entry.Value)
	assert.Equal(t, key, entry.Key)
}

func TestDBDeleteOperation(t *testing.T) {
	dir := dbTestPath
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()

	key := []byte("test-key")
	value := []byte("test-value")

	// 设置值
	txn := db.NewTransaction(true)
	err = txn.Set(key, value)
	assert.NoError(t, err)
	_, err = txn.Commit()
	assert.NoError(t, err)

	// 验证值存在
	txn2 := db.NewTransaction(false)
	entry, err := txn2.Get(key)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, value, entry.Value)
	txn2.Discard()

	// 删除值
	txn3 := db.NewTransaction(true)
	err = txn3.Delete(key)
	assert.NoError(t, err)
	_, err = txn3.Commit()
	assert.NoError(t, err)

	// 验证值已被删除
	txn4 := db.NewTransaction(false)
	defer txn4.Discard()
	_, err = txn4.Get(key)
	assert.Error(t, err)
	assert.Equal(t, common.ErrKeyNotFound, err)
}

func TestDBBatchOperations(t *testing.T) {
	dir := dbTestPath
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()
	txn := db.NewTransaction(true)
	// 创建多个条目进行批量操作测试
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("batch-key-%d", i))
		value := []byte(fmt.Sprintf("batch-value-%d", i))
		err = txn.Set(key, value)
		assert.NoError(t, err)
	}
	_, err = txn.Commit()
	assert.NoError(t, err)

	txn1 := db.NewTransaction(false)
	// 验证所有条目都被正确设置
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("batch-key-%d", i))
		entry, err := txn1.Get(key)
		assert.NoError(t, err)
		assert.NotNil(t, entry)
		assert.Equal(t, fmt.Sprintf("batch-value-%d", i), string(entry.Value))
		txn.Discard()
	}
}

func TestIterator(t *testing.T) {
	dir := dbTestPath
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()

	txn := db.NewTransaction(true)
	// 插入一些数据
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err = txn.Set(key, value)
		assert.NoError(t, err)
	}
	_, err = txn.Commit()
	assert.NoError(t, err)

	// 使用事务迭代器遍历数据
	txn1 := db.NewTransaction(false)
	defer txn.Discard()

	iter := txn1.NewIterator(&interfaces.Options{IsAsc: true, IsSetCache: false})
	defer func() {
		err := iter.Close()
		assert.NoError(t, err)
	}()

	count := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		assert.NotNil(t, item.Item)
		assert.NotNil(t, item.Item.Key)
		assert.NotNil(t, item.Item.Value)
		count++
	}

	assert.Equal(t, 5, count)
}

func TestDBMaxVersion(t *testing.T) {
	dir := dbTestPath
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()

	// 初始版本号应该是0
	initialVersion := db.MaxVersion()
	assert.Equal(t, uint64(0), initialVersion)

	// 插入一些数据
	key := []byte("version-test-key")
	value := []byte("version-test-value")

	txn := db.NewTransaction(true)
	err = txn.Set(key, value)
	assert.NoError(t, err)
	_, err = txn.Commit()
	assert.NoError(t, err)

	// 版本号应该增加
	newVersion := db.MaxVersion()
	assert.True(t, newVersion > initialVersion)
}

func TestDBValueLogGC(t *testing.T) {
	dir := dbTestPath
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	opt.ValueThreshold = 100 // 100B
	// 设置较小的值日志文件大小以便测试GC
	opt.ValueLogFileSize = 1 << 20 // 1MB
	opt.ValueLogMaxEntries = 100

	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
		_ = callBack()
	}()

	// 插入一些大数据条目以填充值日志
	for i := 0; i < 300; i++ {
		key := []byte(fmt.Sprintf("gc-key-%d", i))
		// 创建较大的值以确保存储在值日志中
		value := make([]byte, 20000) // 20KB
		for j := range value {
			value[j] = byte(j % 256)
		}
		txn := db.NewTransaction(true)
		err = txn.Set(key, value)
		assert.NoError(t, err)
		_, err = txn.Commit()
		assert.NoError(t, err)
	}

	// 尝试运行值日志GC
	err = db.RunValueLogGC(0.5) // 50%的废弃比例
	// GC可能不会立即运行，这取决于实际的废弃比例
	// 我们只验证它不会返回错误
	if err != nil {
		assert.Equal(t, "cannot run vlog garbage collection because the vlog is not opened", err.Error())
	}
}

func TestDBConcurrentOperations(t *testing.T) {
	dir := dbTestPath
	removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, _ := Open(opt)
	require.NoError(t, err)
	defer db.Close()

	var wg sync.WaitGroup
	n := 10
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("concurrent-key-%d", i))
			value := []byte(fmt.Sprintf("concurrent-value-%d", i))

			txn := db.NewTransaction(true)
			require.NoError(t, txn.Set(key, value))
			_, err := txn.Commit()
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("concurrent-key-%d", i))
		txn := db.NewTransaction(false)
		entry, err := txn.Get(key)
		require.NoError(t, err)
		require.Equal(t,
			fmt.Sprintf("concurrent-value-%d", i),
			string(entry.Value),
		)
		txn.Discard()
	}
}
