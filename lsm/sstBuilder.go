package lsm

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"unsafe"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/pb"
	"github.com/kebukeYi/TrainKV/v2/utils"
)

type SstBuilder struct {
	sstSize       int64
	opt           *Options
	blockList     []*block
	curBlock      *block
	keyCount      uint32
	keyHashes     []uint32 // sst 为单位
	maxVersion    uint64
	baseKey       []byte
	staleDataSize int64
	estimateSize  int64
}

type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int64
}

type block struct {
	offset          int
	checkSum        []byte
	chkLen          int
	entriesIndexOff int
	data            []byte
	baseKey         []byte
	entryOffsets    []uint32 // restart Point Sets;
	endOffset       int
	estimateSize    int64
}

func (b *block) verifyCheckSum() error {
	return utils.VerifyChecksum(b.data, b.checkSum)
}

type entryHeader struct {
	overlap uint16
	dif     uint16
}

const headerSize = uint16(unsafe.Sizeof(entryHeader{}))

func (h *entryHeader) encode() []byte {
	var buf [headerSize]byte
	*(*entryHeader)(unsafe.Pointer(&buf[0])) = *h
	return buf[:]
}

func (h *entryHeader) decode(buf []byte) {
	arrPtr := (*[headerSize]byte)(unsafe.Pointer(h))
	copy(arrPtr[:], buf[:headerSize])
}

func newSSTBuilderWithSSTableSize(opt *Options, size int64) *SstBuilder {
	size = 2 * size
	if size > common.MaxAllocatorInitialSize {
		size = common.MaxAllocatorInitialSize
	}
	return &SstBuilder{
		opt:     opt,
		sstSize: int64(size),
	}
}

func NewSSTBuilder(opt *Options) *SstBuilder {
	return &SstBuilder{
		opt:     opt,
		sstSize: opt.BaseTableSize,
	}
}

func (ssb *SstBuilder) AddKey(e *model.Entry) {
	ssb.Add(e, false)
}

func (ssb *SstBuilder) AddStaleKey(e *model.Entry) {
	// staleDataSize 可作为 sst 合并时的分数;
	ssb.staleDataSize += int64(len(e.Key) + len(e.Value) + 4 /* entry offset */ + 4 /* header size */)
	ssb.Add(e, true)
}

func (ssb *SstBuilder) Add(e *model.Entry, isStale bool) {
	keyTs := e.Key
	val := model.ValueExt{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	// 检查是否需要分配一个新的 block;
	if ssb.tryNewBlock(e) {
		if isStale {
			// 因为在 tableIndex中也有一份;
			ssb.staleDataSize += int64(len(keyTs) + 4 /* len */ + 4 /* offset */)
		}
		ssb.finishBlock()
		ssb.curBlock = &block{
			data: make([]byte, ssb.opt.BlockSize),
		}
	}
	// todo 当前 sst bloom 中 加入 祛除 Key 的 Ts版本号;
	ssb.keyHashes = append(ssb.keyHashes, utils.Hash(model.ParseKey(keyTs)))

	// 提取出真实的递增 commitTs 作为 version;
	if version := model.ParseTsVersion(keyTs); version > ssb.maxVersion {
		ssb.maxVersion = version
	}

	// baseKey:  keyTs
	// 按照 block 为单位 构建 baseKey;
	var diffKey []byte
	if len(ssb.curBlock.baseKey) == 0 {
		ssb.curBlock.baseKey = append(ssb.curBlock.baseKey, keyTs...)
		diffKey = keyTs
	} else {
		diffKey = ssb.keyDiff(keyTs)
	}

	common.CondPanicMessage(!(len(keyTs)-len(diffKey) <= math.MaxUint16),
		"tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16")

	common.CondPanicMessage(!(len(diffKey) <= math.MaxUint16),
		"tableBuilder.add: len(diffKey) <= math.MaxUint16")

	header := &entryHeader{
		overlap: uint16(len(keyTs) - len(diffKey)),
		dif:     uint16(len(diffKey)),
	}

	// 记录每一个 key 的位置, 所有单个entry来构建restart Point[];
	ssb.curBlock.entryOffsets = append(ssb.curBlock.entryOffsets, uint32(ssb.curBlock.endOffset))
	ssb.append(header.encode())
	ssb.append(diffKey)
	buf := ssb.allocate(int(val.EncodeValSize()))
	val.EncodeVal(buf)
}

func (ssb *SstBuilder) append(data []byte) {
	dst := ssb.allocate(len(data))
	common.CondPanicMessage(len(data) != copy(dst, data), "sstBuilder.append data failed")
}

func (ssb *SstBuilder) allocate(need int) []byte {
	curb := ssb.curBlock
	if len(curb.data[curb.endOffset:]) < need {
		sz := 2 * len(curb.data)
		if curb.endOffset+need > sz {
			sz = curb.endOffset + need
		}
		tmp := make([]byte, sz)
		copy(tmp, curb.data)
		curb.data = tmp
	}
	curb.endOffset += need
	return curb.data[curb.endOffset-need : curb.endOffset]
}

func (ssb *SstBuilder) tryNewBlock(e *model.Entry) bool {
	if ssb.curBlock == nil {
		return true
	}
	if len(ssb.curBlock.entryOffsets) == 0 {
		return false
	}

	sz := uint32((len(ssb.curBlock.entryOffsets)+1)*4 + 4 + 8 + 4)
	common.CondPanicMessage(!(sz < math.MaxUint32), "block size too large,integer overflow!")

	// (endOffset+1)*4+ len(key)+len(value)
	entriesOffsetsSize := int64((len(ssb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length

	ssb.curBlock.estimateSize = int64(ssb.curBlock.endOffset) + int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(e.EncodeSize()) + entriesOffsetsSize
	common.CondPanicMessage(!(uint64(ssb.curBlock.endOffset)+uint64(ssb.curBlock.estimateSize) <
		math.MaxUint32), "curBlock.endOffset overflow")

	return ssb.curBlock.estimateSize > int64(ssb.opt.BlockSize)
}

func (ssb *SstBuilder) keyDiff(keyTs []byte) []byte {
	var i int
	for i = 0; i < len(keyTs) && i < len(ssb.curBlock.baseKey); i++ {
		if keyTs[i] != ssb.curBlock.baseKey[i] {
			break
		}
	}
	return keyTs[i:]
}

func (ssb *SstBuilder) flush(lm *LevelsManger, tableName string) (t *Table, err error) {
	bd := ssb.done()
	fid := utils.FID(tableName)
	t = &Table{lm: lm, fid: fid, Name: strconv.FormatUint(fid, 10) + SSTableName}
	t.sst = OpenSStable(&utils.FileOptions{
		FileName: tableName,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int32(bd.size),
		FID:      t.fid,
	})
	buf := make([]byte, bd.size)
	written := bd.copy(buf)
	common.CondPanicMessage(written != len(buf), "tableBuilder.flush written != len(buf)")
	mmapBuf, err := t.sst.Bytes(0, int(bd.size))
	if err != nil {
		return nil, err
	}
	// copy 之前 文件建立好了, 但是数据还没复制完毕, 宕机了; 怎么办?
	copy(mmapBuf, buf)
	err = t.sst.SyncFile()
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (ssb *SstBuilder) done() buildData {
	ssb.finishBlock()
	if len(ssb.blockList) == 0 {
		return buildData{}
	}
	bd := buildData{
		blockList: ssb.blockList,
	}
	var filter utils.Filter
	if ssb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(ssb.keyHashes), ssb.opt.BloomFalsePositive)
		filter = utils.NewFilter(ssb.keyHashes, bits)
	}
	blockIndex, dataSize := ssb.buildBlockIndex(filter)
	checksum := ssb.calculateChecksum(blockIndex)
	bd.index = blockIndex
	bd.checksum = checksum
	bd.size = int64(int(dataSize) + len(blockIndex) + len(checksum) + 4 /* len(blockIndex) */ + 4 /* len(checksum) */)
	return bd
}

func (ssb *SstBuilder) Finish() []byte {
	// 构建 table 的数据;
	bd := ssb.done()
	buf := make([]byte, bd.size)
	written := bd.copy(buf)
	common.CondPanicMessage(written != len(buf), "tableBuilder.flush written != len(buf)")
	return buf
}

func (ssb *SstBuilder) buildBlockIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = ssb.keyCount
	// 1. flush 时, 添加 maxVersion
	// 2. compact 时, 记录 maxVersion
	tableIndex.MaxVersion = ssb.maxVersion
	tableIndex.Offsets = ssb.writeBlockList()
	// stale 数据量必须持久化进索引, 否则 Lmax→Lmax 合并无从判断 (此前恒为 0);
	tableIndex.StaleDataSize = uint32(ssb.staleDataSize)
	var dataBlockSize uint32
	for i := 0; i < len(ssb.blockList); i++ {
		dataBlockSize += uint32(ssb.blockList[i].endOffset)
	}
	marshal, err := tableIndex.Marshal()
	common.Panic(err)
	return marshal, dataBlockSize
}

func (ssb *SstBuilder) writeBlockList() []*pb.BlockOffset {
	var startOffset uint32
	var blockOffsets []*pb.BlockOffset
	for _, bl := range ssb.blockList {
		blockOffset := &pb.BlockOffset{}

		blockOffset.Key = bl.baseKey
		blockOffset.Offset = uint64(startOffset)
		blockOffset.Size_ = uint32(bl.endOffset)

		blockOffsets = append(blockOffsets, blockOffset)
		startOffset += uint32(bl.endOffset)
	}
	return blockOffsets
}

// 将当前 curBlock 进行收尾,主要是 restart Point[],但是并没有进行填充;
func (ssb *SstBuilder) finishBlock() {
	if ssb.curBlock == nil || len(ssb.curBlock.entryOffsets) == 0 {
		return
	}

	// 将当前 block 的元信息 打包进去;
	ssb.append(model.U32SliceToBytes(ssb.curBlock.entryOffsets))
	ssb.append(model.U32ToBytes(uint32(len(ssb.curBlock.entryOffsets))))

	// crc 8B
	checksum := ssb.calculateChecksum(ssb.curBlock.data[:ssb.curBlock.endOffset])

	ssb.append(checksum)
	ssb.append(model.U32ToBytes(uint32(len(checksum))))

	ssb.estimateSize += ssb.curBlock.estimateSize
	ssb.blockList = append(ssb.blockList, ssb.curBlock)
	ssb.keyCount += uint32(len(ssb.curBlock.entryOffsets))
	ssb.curBlock = nil
	return
}

func (ssb *SstBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return model.U64ToBytes(checkSum)
}

func (bd *buildData) copy(buf []byte) int {
	var written int
	for _, block := range bd.blockList {
		written += copy(buf[written:], block.data[:block.endOffset])
	}

	written += copy(buf[written:], bd.index)
	written += copy(buf[written:], model.U32ToBytes(uint32(len(bd.index)))) // 4B

	written += copy(buf[written:], bd.checksum)
	written += copy(buf[written:], model.U32ToBytes(uint32(len(bd.checksum)))) // 4B

	return written
}

func (ssb *SstBuilder) empty() bool {
	return len(ssb.keyHashes) == 0
}

func (ssb *SstBuilder) close() bool {
	return len(ssb.keyHashes) == 0
}

func (ssb *SstBuilder) ReachedCapacity() bool {
	return ssb.estimateSize > ssb.sstSize
}

// 3. 建立block 容器的 迭代器
type blockIterator struct {
	block        *block // baseKey, data , entryOffsets[]
	data         []byte
	idx          int
	baseKey      []byte // 每一个 block都含有一个 baseKey
	key          []byte
	val          []byte
	entryOffsets []uint32
	err          error

	tableID     uint64
	blockID     int
	prevOverlap uint16 // 同一个 block, 其中的多个 entry 多少都有些关联
	it          interfaces.Item
	arena       *utils.ChunkedArena // Item key 的稳定副本来源 (nil 时退回 SafeCopy);
}

func (itr *blockIterator) setBlock(b *block) {
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.prevOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	// 截取data部分;
	itr.data = b.data[:b.entriesIndexOff]
	// 索引部分;
	itr.entryOffsets = b.entryOffsets
}
func (itr *blockIterator) seekToFirst() {
	itr.setIndex(0)
}
func (itr *blockIterator) seekToLast() {
	itr.setIndex(len(itr.entryOffsets) - 1)
}
func (itr *blockIterator) Seek(key []byte) {
	itr.err = nil
	// 手写二分, 避免 sort.Search 闭包分配; 探测阶段只解码 key 不拷贝;
	lo, hi := 0, len(itr.entryOffsets)
	for lo < hi {
		mid := (lo + hi) / 2
		itr.loadIndex(mid)
		if model.CompareKeyWithTs(itr.key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	// idx = 0 有可能也是不存在值的;(例如寻找最小不存在的值);
	itr.setIndex(lo)
}

// loadIndex 解码第 idx 个 entry 的 key 到 itr.key, 不构造 Item、不做拷贝;
// 二分探测阶段只关心 key 大小关系, 拷贝留到最终的 setIndex;
func (itr *blockIterator) loadIndex(idx int) (entryData []byte, header entryHeader) {
	itr.idx = idx // v2.0
	if idx >= len(itr.entryOffsets) || idx < 0 {
		itr.err = common.ErrBlockEOF
		return nil, header
	}
	itr.err = nil
	// 找到entry data区域;
	startOffset := int(itr.entryOffsets[idx])
	if len(itr.baseKey) == 0 { // 说明当前 block 没有重叠key, 因此直接获得不同的key区间
		var baseHeader entryHeader
		baseHeader.decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.dif]
	}
	var endOffset int
	if idx+1 == len(itr.entryOffsets) {
		endOffset = len(itr.data)
	} else {
		endOffset = int(itr.entryOffsets[itr.idx+1])
	}

	entryData = itr.data[startOffset:endOffset]
	header.decode(entryData)
	// 设置 key 重叠区间;
	if header.overlap > itr.prevOverlap {
		itr.key = append(itr.key[0:itr.prevOverlap], itr.baseKey[itr.prevOverlap:header.overlap]...)
	}
	itr.prevOverlap = header.overlap
	valueOffset := headerSize + header.dif
	diffKey := entryData[headerSize:valueOffset]
	itr.key = append(itr.key[:header.overlap], diffKey...)
	return entryData, header
}

func (itr *blockIterator) setIndex(idx int) {
	entryData, header := itr.loadIndex(idx)
	if itr.err != nil {
		return
	}
	valueOffset := headerSize + header.dif
	eny := model.Entry{} // 空 entry
	if itr.arena != nil {
		// 另外的空间;
		eny.Key = itr.arena.Alloc(len(itr.key))
		copy(eny.Key, itr.key)
	} else {
		eny.Key = model.SafeCopy(eny.Key, itr.key)
	}
	var val model.ValueExt
	val.DecodeVal(entryData[valueOffset:])
	itr.val = val.Value
	eny.Value = itr.val
	eny.ExpiresAt = val.ExpiresAt
	eny.Meta = val.Meta
	eny.Version = model.ParseTsVersion(itr.key)
	itr.it = interfaces.Item{Item: eny}
}
func (itr *blockIterator) Name() string {
	str := fmt.Sprintf("BlockIterator.Block %d", itr.blockID)
	return str
}
func (itr *blockIterator) Next() {
	itr.setIndex(itr.idx + 1)
}

func (itr *blockIterator) Prev() {
	itr.setIndex(itr.idx - 1)
}

func (itr *blockIterator) Valid() bool {
	return !errors.Is(itr.err, common.ErrBlockEOF)
}

func (itr *blockIterator) Rewind() {
	itr.setIndex(0)
}

func (itr *blockIterator) Item() interfaces.Item {
	return itr.it
}

func (itr *blockIterator) Close() error {
	itr.block = nil
	itr.data = nil
	itr.err = nil
	itr.baseKey = nil
	itr.key = nil
	itr.val = nil
	itr.entryOffsets = nil
	itr.arena = nil
	return nil
}

func (itr *blockIterator) Error() error {
	return itr.err
}
