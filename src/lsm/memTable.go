package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	errors "trainKv/common"
	"trainKv/model"
	. "trainKv/utils"
)

type memoryTable struct {
	lsm      *LSM
	skipList *SkipList
	wal      *WAL
}

func (lsm *LSM) NewMemoryTable() *memoryTable {
	newFid := atomic.AddUint64(&(lsm.levelManger.maxFID), 1)
	walFileOpt := &model.FileOptions{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize), // wal 要设置多大比较合理？ 姑且跟sst一样大
		FID:      newFid,
		FileName: mtFilePath(lsm.option.WorkDir, newFid),
	}
	return &memoryTable{
		lsm:      lsm,
		skipList: NewSkipList(lsm.option.MemTableSize),
		wal:      OpenWalFile(walFileOpt),
	}
}

func (m *memoryTable) Get(key []byte) (*model.Entry, error) {
	if key == nil {
		return nil, errors.ErrNotFound
	}
	val := m.skipList.Get(key)
	if val.Value == nil {
		return nil, errors.ErrNotFound
	}
	e := &model.Entry{
		Key:       key,
		Value:     val.Value,
		Meta:      val.Meta,
		ExpiresAt: val.ExpiresAt,
		Version:   val.Version,
	}
	return e, nil
}

func (m *memoryTable) Put(e *model.Entry) error {
	if err := m.wal.Write(e); err != nil {
		return err
	}
	m.skipList.Put(e)
	return nil
}

func (m *memoryTable) Size() int64 {
	return m.skipList.GetMemSize()
}

func (m memoryTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
	return nil
}

func (lsm *LSM) recovery() (*memoryTable, []*memoryTable) {
	files, err := os.ReadDir(lsm.option.WorkDir)
	if err != nil {
		errors.Panic(err)
		return nil, nil
	}
	var fids []uint64
	mixFid := lsm.levelManger.maxFID
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), walFileExt) {
			continue
		}
		fileNameSize := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fileNameSize-len(walFileExt)], 10, 64)
		if mixFid < fid {
			mixFid = fid
		}
		if err != nil {
			errors.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	var immutable []*memoryTable
	for _, fid := range fids {
		memTable, err := lsm.openMemTable(fid)
		errors.CondPanic(err != nil, err)
		if memTable.skipList.GetMemSize() == 0 {
			continue
		}
		immutable = append(immutable, memTable)
	}
	lsm.levelManger.maxFID = mixFid
	return lsm.NewMemoryTable(), immutable
}

func (lsm *LSM) openMemTable(walFid uint64) (*memoryTable, error) {
	fileOpt := &model.FileOptions{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      walFid,
		FileName: mtFilePath(lsm.option.WorkDir, walFid),
	}
	walFile := OpenWalFile(fileOpt)
	s := NewSkipList(lsm.option.MemTableSize)
	mem := &memoryTable{
		lsm:      lsm,
		skipList: s,
		wal:      walFile,
	}
	err := mem.recovery2SkipList()
	errors.CondPanic(err != nil, err)
	return mem, nil
}

func (m *memoryTable) recovery2SkipList() error {
	if m.wal == nil || m.skipList == nil {
		return nil
	}
	var readAt uint32 = 0
	for {
		var e *model.Entry
		e, readAt = m.wal.Read(readAt)
		if readAt > 0 && e != nil {
			m.skipList.Put(e)
			continue
		} else {
			break
		}
	}
	return nil
}

func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}
