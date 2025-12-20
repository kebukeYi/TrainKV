package TrainKV

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
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
	filesLock          sync.RWMutex // 防止 vlogFile 意外被删除;
	filesMap           map[uint32]*VLogFile
	maxFid             uint32 // vlog 组件的最大id
	FilesToDel         []uint32
	activeIteratorNum  int32
	writableFileOffset uint32
	entriesWrittenNum  int32
	Opt                *lsm.Options
	buf                *bytes.Buffer

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
	atomic.StoreUint32(&vlog.writableFileOffset, endOffset)
	if err = vlog.sendDiscardStats(); err != nil {
		if !errors.Is(err, common.ErrKeyNotFound) {
			common.Panic(fmt.Errorf("Failed to populate discard stats: %s\n", err))
		}
	}
	return nil
}

func (vlog *ValueLog) fillVlogFileMap() error {
	vlog.filesMap = make(map[uint32]*VLogFile)

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
		vlogFile := &VLogFile{FID: uint32(fid), Lock: sync.RWMutex{}}
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

func (vlog *ValueLog) ReadValueBytes(vp *model.ValuePtr) ([]byte, *VLogFile, error) {
	vlogFileLocked, err := vlog.getVlogFileLocked(vp)
	if err != nil {
		return nil, nil, err
	}
	// file.read(), not vlog read;
	buf, err := vlogFileLocked.Read(vp)
	return buf, vlogFileLocked, err
}

func (vlog *ValueLog) getVlogFileLocked(vp *model.ValuePtr) (*VLogFile, error) {
	vlog.filesLock.Lock()
	defer vlog.filesLock.Unlock()
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

func (vlog *ValueLog) getUnlockCallBack(vlogFile *VLogFile) func() {
	if vlogFile == nil {
		return nil
	}
	return vlogFile.Lock.RUnlock
}

func (vlog *ValueLog) validateWrites(reqs []*model.Request) error {
	writableFileOffset := uint64(atomic.LoadUint32(&vlog.writableFileOffset))
	for _, req := range reqs {
		size := estimateRequestSize(req)
		estimatedVlogOffset := writableFileOffset + size
		if estimatedVlogOffset > uint64(vlog.Opt.ValueLogFileMaxSize) {
			return errors.Errorf("Request size offset %d is bigger than maximum offset %d",
				estimatedVlogOffset, vlog.Opt.ValueLogFileMaxSize)
		}
		if estimatedVlogOffset >= uint64(vlog.Opt.ValueLogFileMaxSize) {
			writableFileOffset = 0
			continue
		}
		writableFileOffset = estimatedVlogOffset
	}
	return nil
}
func estimateRequestSize(req *model.Request) uint64 {
	size := uint64(0)
	for _, entry := range req.Entries {
		size += uint64(common.VlogHeaderSize + len(entry.Key) + len(entry.Value) + crc32.Size)
	}
	return size
}
func (vlog *ValueLog) Write(reqs []*model.Request) error {
	if err := vlog.validateWrites(reqs); err != nil {
		return common.Wrap(err, "#Write while validating reqs")
	}
	vlog.filesLock.RLock()
	curVlogFile := vlog.filesMap[vlog.maxFid]
	vlog.filesLock.RUnlock()
	vlog.buf.Reset()

	flushToFile := func() error {
		if vlog.buf.Len() == 0 {
			return nil
		}
		data := vlog.buf.Bytes()
		offset := vlog.getWriteOffset()
		// vlogFile 会自动扩容;
		if err := curVlogFile.Write(offset, data); err != nil {
			return errors.Wrapf(err, "Unable to write to value log file: %q", curVlogFile.FileName())
		}
		vlog.buf.Reset()
		atomic.AddUint32(&vlog.writableFileOffset, uint32(len(data)))
		curVlogFile.SetSize(vlog.writableFileOffset)
		return nil
	}

	toWrite := func() error {
		if err := flushToFile(); err != nil {
			return err
		}
		// 因为 vlogFile 会自动扩容, 因此在 flushToFile() 写完后, 我们还需要再进一步判断是否需要创建新的文件;
		if vlog.getWriteOffset() > uint32(vlog.Opt.ValueLogFileSize) ||
			vlog.entriesWrittenNum > vlog.Opt.ValueLogMaxEntries {
			// 截断当前达到阈值的文件;
			if err := curVlogFile.DoneWriting(vlog.getWriteOffset()); err != nil {
				return err
			}
			newFid := atomic.AddUint32(&vlog.maxFid, 1)
			common.CondPanic(newFid <= 0, fmt.Errorf("vlogFile newid has overflown uint32: %v", newFid))
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
		var writeNums int
		for _, entry := range req.Entries {
			if vlog.Db.ShouldWriteValueToLSM(entry) {
				req.ValPtr = append(req.ValPtr, &model.ValuePtr{})
				continue
			}
			var p model.ValuePtr
			p.Fid = curVlogFile.FID
			p.Offset = vlog.getWriteOffset() + uint32(vlog.buf.Len())
			// 在wal中记录事务标记;
			walMeta := entry.Meta
			// 在vlogFile中, 不记录事务标记;
			entry.Meta = entry.Meta &^ (common.BitTxn | common.BitFinTxn)
			plen, err := curVlogFile.EncodeEntry(entry, vlog.buf)
			// 为了后续的 wal 记录, 需要将事务标记写入;
			entry.Meta = walMeta
			if err != nil {
				return err
			}
			p.Len = uint32(plen)
			req.ValPtr = append(req.ValPtr, &p)
			writeNums++
			// 如果 buf 长度够了, 那么就写入文件;
			if int32(vlog.buf.Len()) > vlog.Db.Opt.ValueLogFileSize {
				if err = flushToFile(); err != nil {
					return err
				}
			}
		}

		vlog.entriesWrittenNum += int32(writeNums)
		writeNow := vlog.getWriteOffset()+uint32(vlog.buf.Len()) > uint32(vlog.Opt.ValueLogFileSize) ||
			vlog.entriesWrittenNum > vlog.Opt.ValueLogMaxEntries
		if writeNow {
			if err := toWrite(); err != nil {
				return nil
			}
		}
	}

	return toWrite()
}

func (vlog *ValueLog) deleteVlogFile(vlogFile *VLogFile) error {
	if vlogFile == nil {
		return nil
	}
	vlogFile.Lock.Lock()
	defer vlogFile.Lock.Unlock()
	// 解除 mmap;
	if err := vlogFile.Close(); err != nil {
		return err
	}
	// 执行物理删除;
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
	// 由于每次启动kv, 都将创建新vlog; 因此, 每次关闭前, 进行判断最新的vlog是否有数据写入, 没有写入的话,那就执行删除掉;
	// 避免 无效文件过多;
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
		vlog.filesLock.Lock()
		defer vlog.filesLock.Unlock()
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
			Key:   model.KeyWithTs([]byte(common.VlogFileDiscardStatsKey), math.MaxUint64),
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

func (vlog *ValueLog) createVlogFile(fid uint32) (*VLogFile, error) {
	fpath := vlog.fpath(fid)
	vlogFile := &VLogFile{FID: fid, Lock: sync.RWMutex{}}
	if err := vlogFile.Open(&utils.FileOptions{
		FID:      uint64(fid),
		FileName: fpath,
		Dir:      vlog.DirPath,
		Path:     vlog.DirPath,
		MaxSz:    2 * vlog.Db.Opt.ValueLogFileSize,
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
	vlog.filesLock.Lock()
	defer vlog.filesLock.Unlock()
	vlog.filesMap[fid] = vlogFile
	vlog.maxFid = fid
	atomic.StoreUint32(&vlog.writableFileOffset, 0) // 新创建的 vlogFile 文件, 无魔数片头, 直接从0开始写入;
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
		vLogFile := vlog.pickVlogFile(discardRatio)
		if vLogFile == nil {
			return common.ErrNoRewrite
		}
		if err = vlog.doRunGC(vLogFile); err == nil {
			return nil
		}
		return err
	default:
		return common.ErrRejected
	}
}

func (vlog *ValueLog) doRunGC(logFile *VLogFile) error {
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

func (vlog *ValueLog) iterator(vlogFile *VLogFile, offset uint32, fn model.LogEntry) (uint32, error) {
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

func (vlog *ValueLog) incrIteratorCount() {
	atomic.AddInt32(&vlog.activeIteratorNum, 1)
}

func (vlog *ValueLog) decrIteratorCount() error {
	num := atomic.AddInt32(&vlog.activeIteratorNum, -1)
	if num != 0 {
		return nil
	}

	vlog.filesLock.Lock()
	lfs := make([]*VLogFile, 0, len(vlog.FilesToDel))
	for _, id := range vlog.FilesToDel {
		lfs = append(lfs, vlog.filesMap[id])
		delete(vlog.filesMap, id)
	}
	vlog.FilesToDel = nil
	vlog.filesLock.Unlock()

	for _, lf := range lfs {
		if err := vlog.deleteVlogFile(lf); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *ValueLog) pickVlogFile(discardRatio float64) *VLogFile {
	vlog.filesLock.Lock()
	defer vlog.filesLock.Unlock()
	// 目前有效的 vlogFile[];
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
		return vlog.filesMap[candidate.fid]
	}
	return nil
}

// GC: 对vlog中的每个kv进行判断: LSM中有的话说明有效数据,就再重新写到新文件中,否则就丢弃掉;
func (vlog *ValueLog) gcReWriteLog(logFile *VLogFile) error {
	vlog.filesLock.RLock()
	maxFid := vlog.maxFid
	vlog.filesLock.RUnlock()
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

		// 当前entry被安排在了新的文件 || 当前entry被安排在了当前文件的后面 offset 处;
		if vp.Fid > logFile.FID || vp.Offset > vlogEntry.Offset {
			return nil
		}

		// 有效entry, 需要转移到新的 vlog 文件中;
		if vp.Fid == logFile.FID && vp.Offset == vlogEntry.Offset {
			moved++
			safeCopy := vlogEntry.SafeCopy()
			es := int64(safeCopy.EstimateSize(vlog.Db.Opt.ValueThreshold))
			es += int64(len(safeCopy.Value))
			if int64(len(tempArray)+1) >= vlog.Opt.MaxBatchCount || size+es >= vlog.Opt.MaxBatchSize {
				if err = vlog.Db.BatchSet(tempArray); err != nil {
					return err
				}
				size = 0
				tempArray = tempArray[:0]
			}
			tempArray = append(tempArray, &safeCopy)
			size += es
		} else {
			// vp.Fid < logFile.FID || vp.Offset < vlogEntry.Offset
			// 假设: vlog_1: oldKey_1;  vlog_9: oldKey_9; vlog_10: oldKey_10;
			// 选中 vlog_1 进行GC; 然后 oldKey_1 就被放到了 level3中的sst中;
			// 然后l0层中 sst_1: oldKey_9;  sst_2: oldKey_10; 发生了合并
			// sst_1 和 sst_2 两个文件一起发生合并时, 由于 numberOfVersionsToKeep=1,
			// 那么在合并时 将会只留下版本最高的oldKey_10, oldKey_9将会被抛弃;
			// 好, 那么现在 vlog_9 进行GC, 那么 txn.get(oldKey_9),将会返回level3中的 oldKey_1;
			// 这就是返回了 小于 fid 的情况; 当前entry 可以被跳过;
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
		vlog.filesLock.Lock()
		if _, ok := vlog.filesMap[logFile.FID]; !ok {
			vlog.filesLock.Unlock()
		}
		if vlog.getIteratorCount() == 0 {
			delete(vlog.filesMap, logFile.FID)
			deleteNow = true
		} else {
			// 不能删除的, 先进行内存保存;
			vlog.FilesToDel = append(vlog.FilesToDel, logFile.FID)
		}
		vlog.filesLock.Unlock()
	}
	if deleteNow {
		if err = vlog.deleteVlogFile(logFile); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *ValueLog) sendDiscardStats() error {
	key := []byte(common.VlogFileDiscardStatsKey)
	keyTs := model.KeyWithTs(key, math.MaxUint64)
	entry, err := vlog.Db.Get(keyTs)
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
