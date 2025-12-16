package common

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var (
	gopath = path.Join(os.Getenv("GOPATH"), "src") + "/"
)

var (
	ErrLockDB        = errors.New("lock database error")
	ErrEmptyKey      = errors.New("key can not be empty")
	ErrOutOffset     = errors.New("out offset")
	ErrNotFoundTable = errors.New("not found table of key")
	ErrKeyNotFound   = errors.New("err not found key")

	ErrWalInvalidCrc = errors.New("walFile: invalid crc")
	ErrBadReadMagic  = errors.New("read magic failed")
	ErrBadMagic      = errors.New("bad magic")
	ErrBadCRC        = errors.New("bad crc")
	ErrBadReadCRC    = errors.New("read crc")
	ErrBadChecksum   = errors.New("bad Checksum from manifestFile")
	ErrBadRemoveSST  = errors.New("while removing table")

	ErrChecksumMismatch = errors.New("checksum mismatch")

	ErrTruncate      = errors.New("err do truncate")
	ErrEmptyVlogFile = errors.New("empty vlogFile when Entry()")

	ErrFillTables = errors.New("unable to fill tables")

	ErrReadOnlyTxn  = errors.New("no sets or deletes are allowed in a read-only transaction")
	ErrDiscardedTxn = errors.New("this transaction has been discarded. Create a new one")

	ErrTxnTooBig = errors.New("txn is too big to fit into one request")
	ErrConflict  = errors.New("transaction Conflict. Please retry")

	ErrBatchTooLarge = errors.New("batch is too big to fit into one request")

	ErrNoRewrite = errors.New("value log GC attempt didn't result in any cleanup")

	ErrRejected = errors.New("value log GC request rejected")
)

func location(deep int, fullPath bool) string {
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}

	if fullPath {
		if strings.HasPrefix(file, gopath) {
			file = file[len(gopath):]
		}
	} else {
		file = filepath.Base(file)
	}
	return file + ":" + strconv.Itoa(line)
}
func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2, true), err)
	}
	return err
}
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}
func Panic2(_ interface{}, err error) {
	Panic(err)
}
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
func Check(err error) {
	if err != nil {
		log.Fatalf("%+v", Wrap(err, ""))
	}
}
func Wrap(err error, msg string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s err: %+v", msg, err)
}
func WarpErr(format string, err error) error {
	if err != nil {
		fmt.Printf("%s %s %s", format, location(2, true), err)
	}
	return err
}
