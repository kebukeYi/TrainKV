package common

import (
	"errors"
	"fmt"
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
	ErrDeleteVlogFile = errors.New("Delete vlog file")
	ErrEmptyKey       = errors.New("Key cannot be empty")
	ErrNotFound       = errors.New("not found")
	ErrOutOffset      = errors.New("not found")
	ErrNotFoundTable  = errors.New("not found table of key")
	ErrKeyNotFound    = errors.New("err not found key")
	ErrWalInvalidCrc  = errors.New("walFile: invalid crc")
	ErrBadReadMagic   = errors.New("read magic failed")
	ErrBadMagic       = errors.New("bad magic")
	ErrBadCRC         = errors.New("bad crc")
	ErrBadReadCRC     = errors.New("read crc")
	ErrBadChecksum    = errors.New("bad Checksum from manifestFile")
	ErrBadRemoveSST   = errors.New("While removing table")
	//ErrChecksumMismatch is returned at checksum mismatch.
	ErrChecksumMismatch = errors.New("checksum mismatch")

	ErrTruncate      = errors.New("Do truncate")
	ErrEmptyVlogFile = errors.New("Empty vlogFile when Entry()")
	ErrStop          = errors.New("Stop")
	ErrfillTables    = errors.New("Unable to fill tables")

	ErrBlockedWrites = errors.New("Writes are blocked, possibly due to DropAll or Close")
	ErrTxnTooBig     = errors.New("Txn is too big to fit into one request")
	ErrBatchTooLarge = errors.New("Batch is too big to fit into one request")
	ErrNoRoom        = errors.New("No room for write")

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = errors.New("Invalid request")
	// ErrNoRewrite is returned if a call for value log GC doesn't result in a log file rewrite.
	ErrNoRewrite = errors.New("Value log GC attempt didn't result in any cleanup")

	// ErrRejected is returned if a value log GC is called either while another GC is running, or
	// after DB::Close has been called.
	ErrRejected = errors.New("Value log GC request rejected")
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

func PrintErr(err error, str string) {
	if err != nil {
		fmt.Printf("%s: %s\n", str, err)
	}
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
func WarpErr(format string, err error) error {
	if err != nil {
		fmt.Printf("%s %s %s", format, location(2, true), err)
	}
	return err
}
