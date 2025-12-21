package TrainKV

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func removeAll(dir string) {
	_ = os.RemoveAll(dir)
}

func TestTransactionBasic(t *testing.T) {
	dir, err := os.MkdirTemp("", "trainkv-test")
	require.NoError(t, err)
	defer removeAll(dir)
	open := time.Now()
	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		db.Close()
		_ = callBack()
	}()
	fmt.Println("TrainKV open time: ", time.Since(open))

	tx1 := time.Now()
	// Create a transaction
	txn := db.NewTransaction(true) // true for update transaction
	require.NotNil(t, txn)

	// set some values
	err = txn.Set([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)

	err = txn.Set([]byte("key2"), []byte("value2"))
	assert.NoError(t, err)

	// Commit the transaction
	_, err = txn.Commit()
	assert.NoError(t, err)
	fmt.Println("TrainKV commit time: ", time.Since(tx1))

	tx2 := time.Now()
	// Create another transaction to read the values
	txn2 := db.NewTransaction(false) // false for read-only transaction
	defer txn2.Discard()

	// Read values
	entry, err := txn2.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry.Value)

	entry, err = txn2.Get([]byte("key2"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value2"), entry.Value)
	fmt.Println("TrainKV read time: ", time.Since(tx2))
}

func TestTransactionDiscard(t *testing.T) {
	dir, err := os.MkdirTemp("", "trainkv-test")
	require.NoError(t, err)
	defer removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		db.Close()
		_ = callBack()
	}()

	// Create a transaction
	txn := db.NewTransaction(true)
	require.NotNil(t, txn)

	// set some values
	err = txn.Set([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)

	// Discard the transaction (rollback)
	txn.Discard()

	// Create another transaction to verify the value was not committed
	txn2 := db.NewTransaction(false)
	defer txn2.Discard()

	// Try to read the value - should not exist
	_, err = txn2.Get([]byte("key1"))
	assert.Error(t, err)
	assert.Equal(t, common.ErrKeyNotFound, err)
}

func TestTransactionDelete(t *testing.T) {
	dir, err := os.MkdirTemp("", "trainkv-test")
	require.NoError(t, err)
	defer removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		db.Close()
		_ = callBack()
	}()

	// First, insert a value
	txn1 := db.NewTransaction(true)
	err = txn1.Set([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)
	_, err = txn1.Commit()
	assert.NoError(t, err)

	// Now delete it in another transaction
	txn2 := db.NewTransaction(true)
	err = txn2.Delete([]byte("key1"))
	assert.NoError(t, err)
	_, err = txn2.Commit()
	assert.NoError(t, err)

	// Try to read the value - should not exist
	txn3 := db.NewTransaction(false)
	defer txn3.Discard()

	_, err = txn3.Get([]byte("key1"))
	assert.Error(t, err)
	assert.Equal(t, common.ErrKeyNotFound, err)
}

func TestTransactionConflictDetection(t *testing.T) {
	dir, err := os.MkdirTemp("", "trainkv-test")
	require.NoError(t, err)
	defer removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		db.Close()
		_ = callBack()
	}()

	// set value;
	txn0 := db.NewTransaction(true)
	err = txn0.Set([]byte("key1"), []byte("value0"))
	assert.NoError(t, err)
	_, err = txn0.Commit()
	assert.NoError(t, err)

	// Create two concurrent transactions
	txn1 := db.NewTransaction(true)
	txn2 := db.NewTransaction(true)

	// Both transactions read the same key
	_, err1 := txn1.Get([]byte("key1"))
	_, err2 := txn2.Get([]byte("key1"))

	// One of them sets a value
	err = txn1.Set([]byte("key1"), []byte("value1"))
	assert.NoError(t, err1)
	assert.NoError(t, err)

	// Commit the first transaction
	_, err = txn1.Commit()
	assert.NoError(t, err)

	// The second transaction tries to set a value on the same key
	err = txn2.Set([]byte("key1"), []byte("value2"))
	assert.NoError(t, err2)
	assert.NoError(t, err)

	// The second transaction should conflict when committing
	_, err = txn2.Commit()
	assert.Error(t, err)
	assert.Equal(t, common.ErrConflict, err)
}

func TestTransactionReadYourWrites(t *testing.T) {
	dir, err := os.MkdirTemp("", "trainkv-test")
	require.NoError(t, err)
	defer removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		db.Close()
		_ = callBack()
	}()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	// set a value
	err = txn.Set([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)

	// Read the value within the same transaction - should see the write
	entry, err := txn.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry.Value)

	// Commit
	_, err = txn.Commit()
	assert.NoError(t, err)
}

func TestTransactionBatchOperations(t *testing.T) {
	dir, err := os.MkdirTemp("", "trainkv-test")
	require.NoError(t, err)
	defer removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		db.Close()
		_ = callBack()
	}()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	// Perform multiple operations
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		err = txn.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Commit all at once
	_, err = txn.Commit()
	assert.NoError(t, err)

	// Verify all values were written
	txn2 := db.NewTransaction(false)
	defer txn2.Discard()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)

		entry, err := txn2.Get([]byte(key))
		assert.NoError(t, err)
		assert.Equal(t, []byte(expectedValue), entry.Value)
	}
}

func TestTransactionEmptyKey(t *testing.T) {
	dir, err := os.MkdirTemp("", "trainkv-test")
	require.NoError(t, err)
	defer removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		db.Close()
		_ = callBack()
	}()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	// Try to set an empty key
	err = txn.Set([]byte(""), []byte("value"))
	assert.Error(t, err)
	assert.Equal(t, common.ErrEmptyKey, err)

	// Try to get an empty key
	_, err = txn.Get([]byte(""))
	assert.Error(t, err)
	assert.Equal(t, common.ErrEmptyKey, err)

	// Try to delete an empty key
	err = txn.Delete([]byte(""))
	assert.Error(t, err)
	assert.Equal(t, common.ErrEmptyKey, err)
}

func TestTransactionCommitTs(t *testing.T) {
	dir, err := os.MkdirTemp("", "trainkv-test")
	require.NoError(t, err)
	defer removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		db.Close()
		_ = callBack()
	}()

	txn := db.NewTransaction(true)
	err = txn.Set([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)

	// Check that commitTs
	txn1CommitTs, err := txn.Commit()
	assert.True(t, txn1CommitTs > 0)

	// Create another transaction and verify it has a different timestamp
	time.Sleep(10 * time.Millisecond) // Ensure different timestamp
	txn2 := db.NewTransaction(true)
	err = txn2.Set([]byte("key1"), []byte("value2"))
	assert.NoError(t, err)
	txn2CommitTs, err := txn2.Commit()
	assert.True(t, txn2CommitTs > 0)
	assert.True(t, txn2CommitTs > txn1CommitTs)
}

func TestTransactionModifyReadOnly(t *testing.T) {
	dir, err := os.MkdirTemp("", "trainkv-test")
	require.NoError(t, err)
	defer removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		db.Close()
		_ = callBack()
	}()

	// Create a read-only transaction
	txn := db.NewTransaction(false) // false for read-only
	defer txn.Discard()

	// Try to set a value - should fail
	err = txn.Set([]byte("key1"), []byte("value1"))
	assert.Error(t, err)
	assert.Equal(t, common.ErrReadOnlyTxn, err)

	// Try to delete a value - should fail
	err = txn.Delete([]byte("key1"))
	assert.Error(t, err)
	assert.Equal(t, common.ErrReadOnlyTxn, err)
}

func TestTransactionDiscarded(t *testing.T) {
	dir, err := os.MkdirTemp("", "trainkv-test")
	require.NoError(t, err)
	defer removeAll(dir)

	opt := lsm.GetDefaultOpt(dir)
	db, err, callBack := Open(opt)
	require.NoError(t, err)
	defer func() {
		db.Close()
		_ = callBack()
	}()

	txn := db.NewTransaction(true)

	// Discard the transaction
	txn.Discard()

	// Try to perform operations on discarded transaction
	err = txn.Set([]byte("key1"), []byte("value1"))
	assert.Error(t, err)
	assert.Equal(t, common.ErrDiscardedTxn, err)

	_, err = txn.Get([]byte("key1"))
	assert.Error(t, err)
	assert.Equal(t, common.ErrDiscardedTxn, err)

	err = txn.Delete([]byte("key1"))
	assert.Error(t, err)
	assert.Equal(t, common.ErrDiscardedTxn, err)

	_, err = txn.Commit()
	assert.Error(t, err)
	assert.Equal(t, common.ErrDiscardedTxn, err)
}
