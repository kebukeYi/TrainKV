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

const MemTableName string = ".memtable"

type memoryTable struct {
	lsm         *LSM
	skipList    *SkipList
	wal         *WAL
	maxVersion  uint64
	name        string
	currKey     []byte
	currKeyMeta byte
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
		name:     strconv.FormatUint(newFid, 10) + MemTableName,
	}
}

func (m *memoryTable) Get(key []byte) (*model.Entry, error) {
	if key == nil {
		return nil, errors.ErrNotFound
	}
	val := m.skipList.Get(key) // 没有找到就返回: model.ValueExt{}
	if val.Meta == 0 && val.Value == nil {
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
	m.currKey = e.Key
	m.currKey = e.Key
	m.currKeyMeta = e.Meta
	return nil
}

func (m *memoryTable) Size() int64 {
	return m.skipList.GetMemSize()
}

func (m *memoryTable) close(needRemoveWal bool) error {
	if needRemoveWal || m.wal.Size() == 0 {
		if err := m.wal.CloseAndRemove(); err != nil {
			return err
		}
	} else {
		if err := m.wal.Close(); err != nil {
			return err
		}
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
		//if !strings.HasPrefix(file.Name(), walFileExt) {
		if !strings.HasSuffix(file.Name(), walFileExt) {
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
		name:     strconv.FormatUint(walFid, 10) + MemTableName,
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
	//reader := bufio.NewReader(m.wal.file.NewReader(int(0))) // error
	//reader := model.NewHashReader(m.wal.file.Fd) // ok
	for {
		var e *model.Entry
		e, readAt = m.wal.Read(m.wal.file.Fd)
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
