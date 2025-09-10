package TrainKV

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/file"
	"github.com/kebukeYi/TrainKV/lsm"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/utils"
	"github.com/pkg/errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const discardStatsFlushThreshold = 100

type ValueLog struct {
	DirPath            string
	Mux                sync.RWMutex
	filesMap           map[uint32]*file.VLogFile
	maxFid             uint32 // vlog 组件的最大id
	FilesToDel         []uint32
	activeIteratorNum  int32
	writableFileOffset uint32
	entriesWrittenNum  int32
	Opt                *lsm.Options

	Db                     *TrainKV
	GarbageCh              chan struct{}
	VLogFileDisCardStaInfo *VLogFileDisCardStaInfo
	closer                 *utils.Closer
}

type VLogFileDisCardStaInfo struct {
	mux               sync.RWMutex
	FileMap           map[uint32]int64
	FlushCh           chan map[uint32]int64
	UpdatesSinceFlush int // flush 次数
}

func (vlog *ValueLog) Open(replayFn model.LogEntry) error {
	go vlog.handleDiscardStats()
	if err := vlog.fillVlogFileMap(); err != nil {
		return err
	}
	if len(vlog.filesMap) == 0 {
		_, err := vlog.createVlogFile(1) // 无魔数,直接从vlog文件offset=0,开始写数据;
		return common.WarpErr("Error while creating log file in valueLog.open", err)
	}
	files := vlog.sortedFiles()
	for _, fid := range files {
		lf, ok := vlog.filesMap[fid]
		common.CondPanic(!ok, fmt.Errorf("vlog.filesMap[fid] fid not found"))
		var err error
		if err = lf.Open(&utils.FileOptions{
			FID:      uint64(fid),
			FileName: vlog.fpath(fid),
			Dir:      vlog.DirPath,
			Path:     vlog.DirPath,
			MaxSz:    vlog.Db.Opt.ValueLogFileSize,
		}); err != nil {
			return errors.Wrapf(err, "Open existing file: %q", lf.FileName())
		}
		// 如果当前文件不是 最后一个文件, 执行属性赋值操作;
		if fid < vlog.maxFid {
			if err = lf.Init(); err != nil {
				return err
			}
		}
	}
	lastVLogFile, ok := vlog.filesMap[vlog.maxFid]
	common.CondPanic(!ok, errors.New("vlog.filesMap[vlog.maxFid] not found"))
	endOffset, err := vlog.iterator(lastVLogFile, 0, replayFn)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("file.Seek to end path:[%s]", lastVLogFile.FileName()))
	}
	vlog.writableFileOffset = endOffset
	if err = vlog.sendDiscardStats(); err != nil {
		if !errors.Is(err, common.ErrKeyNotFound) {
			common.Panic(fmt.Errorf("Failed to populate discard stats: %s\n", err))
		}
	}
	return nil
}

func (vlog *ValueLog) fillVlogFileMap() error {
	vlog.filesMap = make(map[uint32]*file.VLogFile)

	vlogFiles, err := os.ReadDir(vlog.DirPath)
	if err != nil {
		return err
	}

	found := make(map[uint64]bool)
	for _, f := range vlogFiles {
		if !strings.HasSuffix(f.Name(), ".vlog") {
			continue
		}
		fid, err := strconv.ParseUint(f.Name()[0:len(f.Name())-5], 10, 32)
		if err != nil {
			return common.WarpErr(fmt.Sprintf("Unable to parse log id. name:[%s]", f.Name()), err)
		}
		if found[fid] {
			return common.WarpErr(fmt.Sprintf("Duplicate file found. Please delete one. name:[%s]", f.Name()), err)
		}
		found[fid] = true
		vlogFile := &file.VLogFile{FID: uint32(fid), Lock: sync.RWMutex{}}
		vlog.filesMap[uint32(fid)] = vlogFile
		if vlog.maxFid < uint32(fid) {
			vlog.maxFid = uint32(fid)
		}
	}
	return nil
}

func (vlog *ValueLog) sortedFiles() []uint32 {
	toBeDelete := make(map[uint32]bool, 0)
	for _, fid := range vlog.FilesToDel {
		toBeDelete[fid] = true
	}
	ret := make([]uint32, 0, len(vlog.filesMap))
	for fid := range vlog.filesMap {
		if !toBeDelete[fid] {
			ret = append(ret, fid)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

func (vlog *ValueLog) Read(vp *model.ValuePtr) ([]byte, func(), error) {
	buf, vlogFileLocked, err := vlog.ReadValueBytes(vp)
	callBack := vlog.getUnlockCallBack(vlogFileLocked)
	if err != nil {
		return nil, callBack, err
	}
	if vlog.Opt.VerifyValueChecksum {
		hash32 := crc32.New(common.CastigationCryTable)
		if _, err := hash32.Write(buf[:len(buf)-crc32.Size]); err != nil {
			model.RunCallback(callBack)
			return nil, nil, errors.Wrapf(err, "failed to write hash for vp %+v", vp)
		}
		checkSum := buf[len(buf)-crc32.Size:]
		if hash32.Sum32() != binary.BigEndian.Uint32(checkSum) {
			model.RunCallback(callBack)
			return nil, nil, errors.Wrapf(common.ErrChecksumMismatch, "value corrupted for vp: %+v", vp)
		}
	}
	var head model.EntryHeader
	headerLen := head.Decode(buf)
	kvData := buf[headerLen:]
	if uint32(len(kvData)) < head.KLen+head.VLen {
		fmt.Errorf("Invalid read: vp: %+v\n", vp)
		return nil, nil, errors.Errorf("Invalid read: Len: %d read at:[%d:%d]",
			len(kvData), head.KLen, head.KLen+head.VLen)
	}
	return kvData[head.KLen : head.KLen+head.VLen], callBack, nil
}

func (vlog *ValueLog) ReadValueBytes(vp *model.ValuePtr) ([]byte, *file.VLogFile, error) {
	vlogFileLocked, err := vlog.getVlogFileLocked(vp)
	if err != nil {
		return nil, nil, err
	}
	// file.read(), not vlog read;
	buf, err := vlogFileLocked.Read(vp)
	return buf, vlogFileLocked, err
}

func (vlog *ValueLog) getVlogFileLocked(vp *model.ValuePtr) (*file.VLogFile, error) {
	vlog.Mux.Lock()
	defer vlog.Mux.Unlock()
	vLogFile, ok := vlog.filesMap[vp.Fid]
	if !ok {
		return nil, errors.Errorf("file with ID: %d not found", vp.Fid)
	}
	if vp.Fid == vlog.maxFid {
		if vp.Offset >= vlog.getWriteOffset() {
			return nil, errors.Errorf("Invalid value pointer offset: %d greater than current offset: %d", vp.Offset, vlog.writableFileOffset)
		}
	}
	vLogFile.Lock.RLock()
	return vLogFile, nil
}

func (vlog *ValueLog) getUnlockCallBack(vlogFile *file.VLogFile) func() {
	if vlogFile == nil {
		return nil
	}
	return vlogFile.Lock.RUnlock
}

func (vlog *ValueLog) NewValuePtr(entry *model.Entry) (*model.ValuePtr, error) {
	req := RequestPool.Get().(*Request)
	req.Reset()
	req.Entries = []*model.Entry{entry}
	req.Wg.Add(1)
	req.IncrRef()
	defer req.DecrRef()
	err := vlog.Write([]*Request{req})
	return req.ValPtr[0], err
}

func (vlog *ValueLog) Write(reqs []*Request) error {
	vlog.Mux.RLock()
	curVlogFile := vlog.filesMap[vlog.maxFid]
	vlog.Mux.RUnlock()
	var buf bytes.Buffer

	flushToFile := func() error {
		if buf.Len() == 0 {
			return nil
		}
		data := buf.Bytes()
		offset := vlog.getWriteOffset()
		if err := curVlogFile.Write(offset, data); err != nil {
			return errors.Wrapf(err, "Unable to write to value log file: %q", curVlogFile.FileName())
		}
		buf.Reset()
		atomic.AddUint32(&vlog.writableFileOffset, uint32(len(data)))
		curVlogFile.SetSize(vlog.writableFileOffset)
		return nil
	}

	toWrite := func() error {
		if err2 := flushToFile(); err2 != nil {
			return err2
		}
		if vlog.getWriteOffset() > uint32(vlog.Opt.ValueLogFileSize) ||
			vlog.entriesWrittenNum > vlog.Opt.ValueLogMaxEntries {
			// 截断当前达到阈值的文件;
			if err := curVlogFile.DoneWriting(vlog.getWriteOffset()); err != nil {
				return err
			}
			newFid := atomic.AddUint32(&vlog.maxFid, 1)
			common.CondPanic(newFid <= 0, fmt.Errorf("newid has overflown uint32: %v", newFid))
			var err error
			curVlogFile, err = vlog.createVlogFile(newFid)
			if err != nil {
				return err
			}
			atomic.AddInt32(&vlog.Db.logRotates, 1)
		}
		return nil
	}

	for _, req := range reqs {
		req.ValPtr = req.ValPtr[:0]
		var writteNum int
		for _, entry := range req.Entries {
			if vlog.Db.ShouldWriteValueToLSM(entry) {
				req.ValPtr = append(req.ValPtr, &model.ValuePtr{})
				continue
			}
			var p model.ValuePtr
			p.Fid = curVlogFile.FID
			p.Offset = vlog.getWriteOffset() + uint32(buf.Len())
			plen, err := curVlogFile.EncodeEntry(entry, &buf)
			if err != nil {
				return err
			}
			p.Len = uint32(plen)
			req.ValPtr = append(req.ValPtr, &p)
			writteNum++
			if int32(buf.Len()) > vlog.Db.Opt.ValueLogFileSize {
				if err = flushToFile(); err != nil {
					return err
				}
			}
		}

		vlog.entriesWrittenNum += int32(writteNum)
		writeNow := vlog.getWriteOffset()+uint32(buf.Len()) > uint32(vlog.Opt.ValueLogFileSize) || vlog.entriesWrittenNum > vlog.Opt.ValueLogMaxEntries
		if writeNow {
			if err := toWrite(); err != nil {
				return nil
			}
		}
	}

	return toWrite()
}

func (vlog *ValueLog) deleteVlogFile(vlogFile *file.VLogFile) error {
	if vlogFile == nil {
		return nil
	}
	vlogFile.Lock.Lock()
	defer vlogFile.Lock.Unlock()
	if err := vlogFile.Close(); err != nil {
		return err
	}
	if err := os.Remove(vlogFile.FileName()); err != nil {
		return err
	}
	return nil
}

func (vlog *ValueLog) getWriteOffset() uint32 {
	return atomic.LoadUint32(&vlog.writableFileOffset)
}

func (vlog *ValueLog) Close() error {
	if vlog == nil || vlog.Db == nil {
		return nil
	}
	var err error
	maxFid := vlog.maxFid
	for _, vLogFile := range vlog.filesMap {
		vLogFile.Lock.Lock()
		if vLogFile.FID == maxFid {
			if vlog.getWriteOffset() == 0 {
				vLogFile.Lock.Unlock()
				if err = vlog.deleteVlogFile(vLogFile); err != nil {
					return err
				}
				continue
			} else {
				if truncErr := vLogFile.Truncate(int64(vlog.getWriteOffset())); truncErr != nil && err == nil {
					err = truncErr
				}
			}
		}
		if closeErr := vLogFile.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		vLogFile.Lock.Unlock()
	}
	return err
}

func (vlog *ValueLog) handleDiscardStats() {
	mergeStats := func(stateInfos map[uint32]int64) ([]byte, error) {
		vlog.Mux.Lock()
		defer vlog.Mux.Unlock()
		if len(stateInfos) == 0 {
			return nil, nil
		}
		for fid, size := range stateInfos {
			vlog.VLogFileDisCardStaInfo.FileMap[fid] += size
			vlog.VLogFileDisCardStaInfo.UpdatesSinceFlush++
		}
		if vlog.VLogFileDisCardStaInfo.UpdatesSinceFlush > discardStatsFlushThreshold {
			bytes, err := json.Marshal(vlog.VLogFileDisCardStaInfo.FileMap)
			if err != nil {
				return nil, err
			}
			vlog.VLogFileDisCardStaInfo.UpdatesSinceFlush = 0
			return bytes, err
		}
		return nil, nil
	}

	processDiscardStats := func(stateInfos map[uint32]int64) error {
		encodeMap, err := mergeStats(stateInfos)
		if err != nil || encodeMap == nil {
			return err
		}
		entries := []*model.Entry{{
			Key:   model.KeyWithTs([]byte(common.VlogFileDiscardStatsKey)),
			Value: encodeMap,
		}}
		request, err := vlog.Db.SendToWriteCh(entries)
		if err != nil {
			return errors.Wrapf(err, "write discard stats to db")
		}
		return request.Wait()
	}

	for {
		select {
		case stateInfo := <-vlog.VLogFileDisCardStaInfo.FlushCh:
			if err := processDiscardStats(stateInfo); err != nil {
				common.Err(fmt.Errorf("unable to process discardstats with error: %s", err))
			}
		}
	}
}

func (vlog *ValueLog) createVlogFile(fid uint32) (*file.VLogFile, error) {
	fpath := vlog.fpath(fid)
	vlogFile := &file.VLogFile{FID: fid, Lock: sync.RWMutex{}}
	if err := vlogFile.Open(&utils.FileOptions{
		FID:      uint64(fid),
		FileName: fpath,
		Dir:      vlog.DirPath,
		Path:     vlog.DirPath,
		MaxSz:    vlog.Db.Opt.ValueLogFileSize,
	}); err != nil {
		return nil, err
	}
	removeFile := func() {
		common.Err(os.Remove(vlogFile.FileName()))
	}
	if err := utils.SyncDir(vlog.DirPath); err != nil {
		removeFile()
		return nil, common.WarpErr(fmt.Sprintf("Sync value log dir[%s]", vlog.DirPath), err)
	}
	vlog.Mux.Lock()
	defer vlog.Mux.Unlock()
	vlog.filesMap[fid] = vlogFile
	vlog.maxFid = fid
	vlog.writableFileOffset = 0 // 新创建的 vlog文件, 无魔数片头,直接从0开始写入;
	vlog.entriesWrittenNum = 0
	return vlogFile, nil
}

func (vlog *ValueLog) fpath(fid uint32) string {
	return utils.VlogFilePath(vlog.DirPath, fid)
}

func (vlog *ValueLog) waitOnGC(closer *utils.Closer) {
	// 不继续等待 vlogGC 完毕吗? 仅仅是禁止新 GC启动;
	defer closer.Done()
	select {
	case <-closer.CloseSignal:
		// Block any GC in progress to finish, and don't allow any more writes to runGC by filling up
		// the channel of size 1.
		// 装满通道, 禁止vlogGC再启动;
		vlog.GarbageCh <- struct{}{}
	}
}

func (vlog *ValueLog) runGC(discardRatio float64) error {
	select {
	case vlog.GarbageCh <- struct{}{}:
		defer func() {
			<-vlog.GarbageCh
		}()
		var err error
		vLogFiles := vlog.pickVlogFile(discardRatio)
		if vLogFiles == nil || len(vLogFiles) == 0 {
			return common.ErrNoRewrite
		}
		tried := make(map[uint32]bool)
		for _, vLogFile := range vLogFiles {
			if tried[vLogFile.FID] {
				continue
			}
			tried[vLogFile.FID] = true
			if err = vlog.doRunGC(vLogFile); err == nil {
				return nil
			}
		}
		return err
	default:
		return common.ErrRejected
	}
}

func (vlog *ValueLog) doRunGC(logFile *file.VLogFile) error {
	var err error
	defer func() {
		if err == nil {
			vlog.VLogFileDisCardStaInfo.mux.Lock()
			delete(vlog.VLogFileDisCardStaInfo.FileMap, logFile.FID)
			vlog.VLogFileDisCardStaInfo.mux.Unlock()
		}
	}()
	if err = vlog.gcReWriteLog(logFile); err != nil {
		return err
	}
	return nil
}

func (vlog *ValueLog) iterator(vlogFile *file.VLogFile, offset uint32, fn model.LogEntry) (uint32, error) {
	if offset == 0 {
		offset = common.VlogHeaderSize
	}
	if int64(offset) == vlogFile.Size() {
		return offset, common.ErrOutOffset
	}

	// We're not at the end of the file. Let's Seek to the offset and start reading.
	if _, err := vlogFile.Seek(int64(offset), io.SeekStart); err != nil {
		return 0, errors.Wrapf(err, "Unable to seek, name:%s", vlogFile.FileName())
	}

	reader := bufio.NewReader(vlogFile.FD())
	var recordEntryOffset uint32 = offset
LOOP:
	for {
		entry, err := vlog.Entry(reader, recordEntryOffset)
		switch {
		case err == io.EOF:
			break LOOP
		case err == io.ErrUnexpectedEOF || err == common.ErrTruncate:
			break LOOP
		case err == common.ErrEmptyVlogFile:
			break LOOP
		case err != nil:
			fmt.Printf("unable to decode entry, err:%v \n", err)
			return recordEntryOffset, err
		}
		//var vp *model.ValuePtr
		var vp model.ValuePtr
		vp.Len = uint32((entry.HeaderLen) + len(entry.Key) + len(entry.Value) + crc32.Size)
		vp.Offset = entry.Offset
		vp.Fid = vlogFile.FID
		recordEntryOffset += vp.Len
		if err = fn(entry, &vp); err != nil {
			return 0, common.WarpErr(fmt.Sprintf("Iteration function %s", vlogFile.FileName()), err)
		}
	}
	return recordEntryOffset, nil
}

func (vlog *ValueLog) Entry(read io.Reader, offset uint32) (*model.Entry, error) {
	hashReader := model.NewHashReader(read)
	var head model.EntryHeader
	hlen, err := head.DecodeFrom(hashReader)
	if err != nil {
		return nil, err
	}

	if head.KLen > uint32(1<<16) {
		return nil, common.ErrTruncate
	}

	// todo 先假设 key 和 value 出现0时,就认为在读取一个新的没有写入数据的空vlog文件;
	if head.KLen == 0 || head.VLen == 0 {
		return nil, common.ErrEmptyVlogFile
	}

	e := &model.Entry{}
	e.Offset = offset
	e.HeaderLen = hlen
	buf := make([]byte, head.KLen+head.VLen)
	if _, err = io.ReadFull(hashReader, buf[:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	e.Key = buf[:head.KLen]
	e.Value = buf[head.KLen:]

	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(read, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	toU32 := model.BytesToU32(crcBuf[:])
	sum32 := hashReader.Sum32()
	if sum32 != toU32 {
		return nil, common.ErrBadCRC
	}
	e.Meta = head.Meta
	e.ExpiresAt = head.ExpiresAt
	return e, nil
}

func (vlog *ValueLog) getIteratorCount() int {
	return int(atomic.LoadInt32(&vlog.activeIteratorNum))
}

func (vlog *ValueLog) pickVlogFile(discardRatio float64) []*file.VLogFile {
	vlog.Mux.Lock()
	defer vlog.Mux.Unlock()
	files := make([]*file.VLogFile, 0)
	sortedFileIDs := vlog.sortedFiles()
	if len(sortedFileIDs) <= 1 {
		return nil
	}

	candidate := struct {
		fid     uint32
		discard int64
	}{math.MaxUint32, 0}

	vlog.VLogFileDisCardStaInfo.mux.RLock()
	for _, sortedFileId := range sortedFileIDs {
		if sortedFileId == vlog.maxFid {
			continue
		}
		if vlog.VLogFileDisCardStaInfo.FileMap[sortedFileId] > candidate.discard {
			candidate.fid = sortedFileId
			candidate.discard = vlog.VLogFileDisCardStaInfo.FileMap[sortedFileId]
		}
	}
	vlog.VLogFileDisCardStaInfo.mux.RUnlock()

	if candidate.fid != math.MaxUint32 {
		lf := vlog.filesMap[candidate.fid]
		fileInfo, _ := lf.FD().Stat()
		if size := discardRatio * float64(fileInfo.Size()); float64(candidate.discard) < size {
			return nil
		}
		files = append(files, vlog.filesMap[candidate.fid])
	}
	return files
}

// GC: LSM中 有的话说明有效数据,就再重新写到新文件中,否则就丢弃掉;
func (vlog *ValueLog) gcReWriteLog(logFile *file.VLogFile) error {
	vlog.Mux.RLock()
	maxFid := vlog.maxFid
	vlog.Mux.RUnlock()
	common.CondPanic((logFile.FID) >= maxFid, fmt.Errorf("fid to move: %d. Current max fid: %d", logFile.FID, maxFid))
	tempArray := make([]*model.Entry, 0, 1000)
	var size int64
	var count, moved int
	fn := func(vlogEntry *model.Entry) error {
		count++
		lsmEntry, err := vlog.Db.Lsm.Get(vlogEntry.Key)
		if err != nil {
			return err
		}
		if model.IsDiscardEntry(vlogEntry, &lsmEntry) {
			return nil
		}

		if lsmEntry.Value == nil || len(lsmEntry.Value) == 0 {
			return errors.Errorf("#gcReWriteLog(): Empty lsmEntry.value: %+v  from lsm;", lsmEntry)
		}

		var vp *model.ValuePtr
		vp.Decode(lsmEntry.Value)

		if vp.Fid > logFile.FID || vp.Offset > vlogEntry.Offset {
			return nil
		}

		//if vp.Fid == logFile.FID && vp.Offset > vlogEntry.Offset {
		if vp.Fid == logFile.FID && vp.Offset == vlogEntry.Offset {
			moved++
			e := &model.Entry{}
			e.Meta = vlogEntry.Meta
			e.ExpiresAt = vlogEntry.ExpiresAt
			e.Key = append([]byte{}, vlogEntry.Key...)
			e.Value = append([]byte{}, vlogEntry.Value...)
			es := int64(e.EstimateSize(vlog.Db.Opt.ValueThreshold))
			es += int64(len(e.Value))
			if int64(len(tempArray)+1) >= vlog.Opt.MaxBatchCount || size+es >= vlog.Opt.MaxBatchSize {
				if err := vlog.Db.BatchSet(tempArray); err != nil {
					return err
				}
				size = 0
				tempArray = tempArray[:0]
			}
			tempArray = append(tempArray, e)
			size += es
		} else {
		}
		return nil
	}

	_, err := vlog.iterator(logFile, 0, func(e *model.Entry, vp *model.ValuePtr) error {
		return fn(e)
	})
	if err != nil {
		return err
	}
	batchSize := 1024
	for i := 0; i < len(tempArray); {
		end := i + batchSize
		if end > len(tempArray) {
			end = len(tempArray)
		}
		if err := vlog.Db.BatchSet(tempArray[i:end]); err != nil {
			if err == common.ErrTxnTooBig {
				batchSize /= 2
				continue
			}
			return err
		}
		i += batchSize
	}

	var deleteNow bool
	{
		vlog.Mux.Lock()
		if _, ok := vlog.filesMap[logFile.FID]; !ok {
			vlog.Mux.Unlock()
		}
		if vlog.activeIteratorNum == 0 {
			delete(vlog.filesMap, logFile.FID)
			deleteNow = true
		} else {
			vlog.FilesToDel = append(vlog.FilesToDel, logFile.FID)
		}
		vlog.Mux.Unlock()
	}
	if deleteNow {
		if err := vlog.deleteVlogFile(logFile); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *ValueLog) sendDiscardStats() error {
	entry, err := vlog.Db.Get([]byte(common.VlogFileDiscardStatsKey))
	if err != nil || entry == nil {
		return err
	}
	val := entry.Value
	if model.IsValPtr(entry) {
		var vp model.ValuePtr
		vp.Decode(val)
		rets, callBack, err := vlog.Read(&vp)
		if err != nil {
			return err
		}
		val = model.SafeCopy(nil, rets)
		model.RunCallback(callBack)
	}
	if len(val) == 0 {
		return nil
	}
	var statMap map[uint32]int64
	if err = json.Unmarshal(val, &statMap); err != nil {
		return errors.Wrapf(err, "failed to unmarshal discard stats")
	}
	fmt.Printf("Value Log Discard stats: %v\n", statMap)
	vlog.VLogFileDisCardStaInfo.FlushCh <- statMap
	return nil
}

var RequestPool = sync.Pool{
	New: func() interface{} {
		return new(Request)
	},
}

type Request struct {
	Entries []*model.Entry
	ValPtr  []*model.ValuePtr
	Wg      sync.WaitGroup
	Err     error
	ref     int32
}

func (r *Request) IncrRef() {
	atomic.AddInt32(&r.ref, 1)
}

func (r *Request) DecrRef() {
	n := atomic.AddInt32(&r.ref, -1)
	if n > 0 {
		return
	}
	r.Entries = nil
	RequestPool.Put(r)
}

func (r *Request) Wait() error {
	r.Wg.Wait()
	err := r.Err
	r.DecrRef()
	return err
}

func (r *Request) Reset() {
	r.Entries = r.Entries[:0]
	r.ValPtr = r.ValPtr[:0]
	r.Wg = sync.WaitGroup{}
	r.Err = nil
	atomic.StoreInt32(&r.ref, 0)
}
