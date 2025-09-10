package lsm

import (
	"fmt"
	errors "github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/utils"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	newFid := lsm.LevelManger.NextFileID()
	walFileOpt := &utils.FileOptions{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int32(lsm.option.MemTableSize),
		FID:      newFid,
		FileName: mtFilePath(lsm.option.WorkDir, newFid),
	}
	mt := &memoryTable{
		lsm:      lsm,
		skipList: NewSkipList(lsm.option.MemTableSize),
		wal:      OpenWalFile(walFileOpt),
		name:     strconv.FormatUint(newFid, 10) + MemTableName,
	}
	mt.skipList.OnClose = func() {
		mt.close(true)
	}
	return mt
}

func (m *memoryTable) Get(keyTs []byte) (model.Entry, error) {
	m.skipList.IncrRef()
	defer m.skipList.DecrRef()
	val := m.skipList.Get(keyTs) // 没有找到就返回: model.ValueExt{Version: -1}
	// 1.没有找到; 满足: val.Version: == -1 && val.Value == nil;
	// 2.delete标记的数据; val.Meta=delete; 仅满足: val.Value == nil;
	if val.Version == -1 {
		return model.Entry{Version: -1}, errors.ErrKeyNotFound
	}
	e := model.Entry{
		Key:       keyTs,
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
	var maxFid uint64
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), walFileExt) {
			continue
		}
		fileNameSize := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fileNameSize-len(walFileExt)], 10, 64)
		if maxFid < fid {
			maxFid = fid
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
	sstMaxID := lsm.LevelManger.maxFID.Load()
	for _, fid := range fids {
		if fid == sstMaxID {
			fmt.Printf("LSM.#recovery(): sstMaxFid(%d.sst) is not allow equal to wal fid(%d.wal)!", sstMaxID, fid)
			// 方式A: 进行删除相关wal文件;
			//if err = os.Remove(mtFilePath(lsm.option.WorkDir, fid)); err != nil {
			//	panic(err)
			//}
			//continue
			// 方式B: 直接退出报错处理;
			panic("LSM.#recovery(): sstMaxFid is not equal to wal fid")
		}
		memTable, err := lsm.openMemTable(fid)
		errors.CondPanic(err != nil, err)
		if memTable.skipList.GetMemSize() == 0 {
			memTable.skipList.DecrRef()
			continue
		}
		immutable = append(immutable, memTable)
	}
	if maxFid > lsm.LevelManger.maxFID.Load() {
		lsm.LevelManger.maxFID.Store(maxFid)
	}
	return lsm.NewMemoryTable(), immutable
}

func (lsm *LSM) openMemTable(walFid uint64) (*memoryTable, error) {
	fileOpt := &utils.FileOptions{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int32(lsm.option.MemTableSize),
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
	mem.skipList.OnClose = func() {
		mem.close(true)
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
