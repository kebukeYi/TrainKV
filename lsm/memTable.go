package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/skl"
	"github.com/kebukeYi/TrainKV/utils"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

const MemTableName string = ".memtable"

type MemoryTable struct {
	lsm        *LSM
	skipList   *skl.SkipList
	wal        *WAL
	maxVersion uint64
	name       string
}

func (lsm *LSM) NewMemoryTable() *MemoryTable {
	newFid := lsm.LevelManger.NextFileID()
	walFileOpt := &utils.FileOptions{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int32(lsm.option.MemTableSize),
		FID:      newFid,
		FileName: mtFilePath(lsm.option.WorkDir, newFid),
	}
	mt := &MemoryTable{
		lsm:      lsm,
		skipList: skl.NewSkipList(lsm.option.MemTableSize),
		wal:      OpenWalFile(walFileOpt),
		name:     strconv.FormatUint(newFid, 10) + MemTableName,
	}
	mt.skipList.OnClose = func() {
		err := mt.close(true)
		common.CondPanic(err != nil, err)
	}
	return mt
}

func (m *MemoryTable) Get(keyTs []byte) (model.Entry, error) {
	m.skipList.IncrRef()
	defer m.skipList.DecrRef()
	val := m.skipList.Get(keyTs) // 没有找到就返回: model.ValueExt{Version: -1}
	// 1.没有找到,需要继续寻找; 满足: val.Version: == -1 && val.Value == nil;
	// 2.delete标记的数据(不再寻找,正确返回); val.Meta=delete; 仅满足: val.Value == nil;
	if val.Version == 0 && val.Value == nil {
		return model.Entry{Version: 0}, common.ErrKeyNotFound
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

func (m *MemoryTable) Put(e *model.Entry) error {
	if err := m.wal.Write(e); err != nil {
		return err
	}
	if e.Meta&common.BitFinTxn > 0 {
		return nil
	}
	m.skipList.Put(e)
	parseKey := model.ParseTsVersion(e.Key)
	if parseKey > m.maxVersion {
		m.maxVersion = parseKey
	}
	return nil
}

func (m *MemoryTable) IncrRef() {
	m.skipList.IncrRef()
}
func (m *MemoryTable) DecrRef() {
	m.skipList.DecrRef()
}

func (m *MemoryTable) Size() int64 {
	return m.skipList.GetMemSize()
}

func (m *MemoryTable) close(needRemoveWal bool) error {
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

func (lsm *LSM) recovery() (*MemoryTable, []*MemoryTable) {
	files, err := os.ReadDir(lsm.option.WorkDir)
	if err != nil {
		common.Panic(err)
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
			common.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	var immutables []*MemoryTable
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
			panic("#recovery(): sstMaxFid is not should equal to wal fid")
		}
		memTable, err := lsm.openMemTable(fid)
		common.CondPanic(err != nil, err)
		if memTable.skipList.Empty() {
			memTable.skipList.DecrRef()
			continue
		}
		immutables = append(immutables, memTable)
	}
	if maxFid > lsm.LevelManger.maxFID.Load() {
		lsm.LevelManger.maxFID.Store(maxFid)
	}
	return lsm.NewMemoryTable(), immutables
}

func (lsm *LSM) openMemTable(walFid uint64) (*MemoryTable, error) {
	fileOpt := &utils.FileOptions{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int32(lsm.option.MemTableSize),
		FID:      walFid,
		FileName: mtFilePath(lsm.option.WorkDir, walFid),
	}
	walFile := OpenWalFile(fileOpt)
	s := skl.NewSkipList(lsm.option.MemTableSize)
	mem := &MemoryTable{
		lsm:      lsm,
		skipList: s,
		wal:      walFile,
		name:     strconv.FormatUint(walFid, 10) + MemTableName,
	}
	mem.skipList.OnClose = func() {
		err := mem.close(true)
		common.CondPanic(err != nil, err)
	}
	endOff, err := mem.recovery2SkipList()
	if err != nil {
		return nil, common.Wrapf(err, "#openMemTable recovery2SkipList end offset: %d < size: %d", endOff, walFile.size)
	}
	if endOff < atomic.LoadUint32(&walFile.size) {
		return nil, common.Wrapf(err, "#openMemTable end offset: %d < size: %d", endOff, walFile.size)
	}
	err = walFile.file.Truncate(int64(endOff))
	return mem, err
}

func (m *MemoryTable) recovery2SkipList() (uint32, error) {
	if m.wal == nil || m.skipList == nil {
		return 0, nil
	}
	var lastCommitTs uint64
	var validEndOffset uint32 = 0
	var readAt uint32 = 0
	entries := make([]*model.Entry, 0)
	for {
		var e *model.Entry
		e, readAt = m.wal.Read(m.wal.file.Fd)
		if e == nil {
			return validEndOffset, nil
		}
		switch {
		case e.Meta&common.BitTxn > 0:
			txnTs := model.ParseTsVersion(e.Key)
			if lastCommitTs == 0 {
				lastCommitTs = txnTs
			}
			if txnTs != lastCommitTs {
				return validEndOffset, common.ErrBadTxn
			}
			entries = append(entries, e)
		case e.Meta&common.BitFinTxn > 0:
			parseUint, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil || parseUint != lastCommitTs {
				return validEndOffset, common.ErrBadTxn
			}
			lastCommitTs = 0
			validEndOffset = readAt
			for _, entry := range entries {
				m.skipList.Put(entry)
			}
			entries = entries[:0]
		default:
			// 非 keepTogether 的数据;
			if lastCommitTs != 0 {
				return validEndOffset, common.ErrBadTxn
			}
			m.skipList.Put(e)
			validEndOffset = readAt
		}
	}
}

func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}
