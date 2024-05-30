package errors

import (
	"errors"
	"fmt"
)

var ErrEmptyKey = errors.New("Key cannot be empty")
var ErrNotFound = errors.New("not found")
var ErrWalInvalidCrc = errors.New("walFile: invalid crc")

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
