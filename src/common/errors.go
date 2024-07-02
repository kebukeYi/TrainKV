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

var ErrEmptyKey = errors.New("Key cannot be empty")
var ErrNotFound = errors.New("not found")
var ErrNotFoundTable = errors.New("not found table of key")
var ErrKeyNotFound = errors.New("not found key ")
var ErrWalInvalidCrc = errors.New("walFile: invalid crc")
var ErrBadReadMagic = errors.New("read magic failed")
var ErrBadMagic = errors.New("bad magic")
var ErrBadCRC = errors.New("bad crc from read manifestFile")
var ErrBadReadCRC = errors.New("read crc failed from manifestFile")
var ErrBadChecksum = errors.New("bad Checksum from manifestFile")
var ErrBadRemoveSST = errors.New("While removing table")

// ErrChecksumMismatch is returned at checksum mismatch.
var ErrChecksumMismatch = errors.New("checksum mismatch")

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

func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
