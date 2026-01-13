//go:build linux
// +build linux

package file

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/v2/mmap"
)

func (m *MmapFile) Truncate(maxSz int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}

	var err error
	m.Buf, err = mmap.Mremap(m.Buf, int(maxSz)) // Mmap up to max size.
	return err
}
