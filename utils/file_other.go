//go:build !windows

package utils

import (
	"fmt"
	"os"
)

func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("while opening %s,err:%w", dir, err)
	}
	if err := df.Sync(); err != nil {
		return fmt.Errorf("while syncing %s,err:%w", dir, err)
	}
	if err := df.Close(); err != nil {
		return fmt.Errorf("while closing %s,err:%w", dir, err)
	}
	return nil
}
