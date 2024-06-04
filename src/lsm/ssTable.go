package lsm

import (
	"fmt"
	"path/filepath"
)

type SSTable struct {
}

func GetSSTablePathFromId(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}
