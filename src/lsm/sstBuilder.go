package lsm

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"sort"
	"trainKv/common"
	"trainKv/model"
	"trainKv/pb"
	"trainKv/utils"
	"unsafe"
)

type sstBuilder struct {
	sstSize       int64
	opt           *Options
	blockList     []*block
	curBlock      *block
	keyCount      uint32
	keyHashes     []uint32 // sst 为单位
	maxVersion    uint64
	baseKey       []byte
	staleDataSize int
	estimateSize  int64
}

type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}

type block struct {
	offset          int
	checkSum        []byte
	chkLen          int
	entriesIndexOff int
	data            []byte
	baseKey         []byte
	entryOffsets    []uint32 // restart Point sets
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
	arrPtr := (*[headerSize]byte)(unsafe.Pointer(&h))
	copy(arrPtr[:], buf[:headerSize])
}

func newSSTBuilderWithSSTableSize(opt *Options, size int64) *sstBuilder {
	return &sstBuilder{
		opt:     opt,
		sstSize: size,
	}
}

func newSSTBuilder(opt *Options) *sstBuilder {
	return &sstBuilder{
		opt: opt,
	}
}

// 1. 向当前block中 append kv数据
func (ssb *sstBuilder) AddKey(e *model.Entry) {
	ssb.add(e, false)
}

func (ssb *sstBuilder) AddStaleKey(e *model.Entry) {
	ssb.staleDataSize += len(e.Key) + len(e.Value) + 4 /* entry offset */ + 4 /* header size */
	ssb.add(e, true)
}

func (ssb *sstBuilder) add(e *model.Entry, isStale bool) {
	key := e.Key
	val := model.ValueExt{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	// 检查是否需要分配一个新的 block
	if ssb.tryNewBlock(e) {
		if isStale {
			ssb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		ssb.finishBlock()
		ssb.curBlock = &block{
			data: make([]byte, ssb.opt.BlockSize),
		}
	}
	// 当前 bloom 中 加入此 Key
	ssb.keyHashes = append(ssb.keyHashes, utils.Hash(model.ParseKey(key)))

	if version := model.ParseTs(key); version > ssb.maxVersion {
		ssb.maxVersion = version
	}
	// 按照 block 为单位 构建 baseKey, 而不是按照16个kv为一组来构建的;
	var diffKey []byte
	if len(ssb.curBlock.baseKey) == 0 {
		//ssb.curBlock.baseKey = append(ssb.curBlock.baseKey, key...)
		ssb.curBlock.baseKey = append(ssb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = ssb.keyDiff(key)
	}
	common.CondPanic(!(len(key)-len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16"))
	common.CondPanic(!(len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16"))
	header := &entryHeader{
		overlap: uint16(len(key) - len(diffKey)),
		dif:     uint16(len(diffKey)),
	}
	// 记录每一个 kv 的位置, 并没有按照16个一组来进行构建, 而是按照单个entry来构建的 restart Point[];
	ssb.curBlock.entryOffsets = append(ssb.curBlock.entryOffsets, uint32(ssb.curBlock.endOffset))
	ssb.append(header.encode())
	ssb.append(diffKey)
	buf := ssb.allocate(int(val.EncodeValSize()))
	val.EncodeVal(buf)
}

func (ssb *sstBuilder) append(data []byte) {
	dst := ssb.allocate(len(data))
	common.CondPanic(len(data) != copy(dst, data),
		errors.New("sstBuilder.append data failed."))
}

func (ssb *sstBuilder) allocate(need int) []byte {
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

func (ssb *sstBuilder) tryNewBlock(e *model.Entry) bool {
	if ssb.curBlock == nil {
		return true
	}
	if len(ssb.curBlock.entryOffsets) <= 0 {
		return false
	}

	sz := uint32((len(ssb.curBlock.entryOffsets)+1)*4 + 4 + 8 + 4)
	common.CondPanic(!(sz < math.MaxUint32),
		errors.New("block size too large,integer overflow!"))

	// |  endOffset 4+ len(key)+len(value)
	entriesOffsetsSize := int64((len(ssb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length

	ssb.curBlock.estimateSize = int64(ssb.curBlock.endOffset) + int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(e.EncodeSize()) + entriesOffsetsSize
	// Integer overflow check for table size.
	common.CondPanic(!(uint64(ssb.curBlock.endOffset)+uint64(ssb.curBlock.estimateSize) <
		math.MaxUint32), errors.New("Integer overflow"))

	return ssb.curBlock.estimateSize > int64(ssb.opt.BlockSize)
}

func (ssb *sstBuilder) keyDiff(key []byte) []byte {
	var i int
	for i = 0; i < len(key) && i < len(ssb.curBlock.baseKey); i++ {
		if key[i] != ssb.curBlock.baseKey[i] {
			break
		}
	}
	return key[i:]
}

// 2. 将当前所有 block 写入文件中
func (ssb *sstBuilder) flush(lm *levelsManger, tableName string) (t *table, err error) {
	bd := ssb.done()
	t = &table{lm: lm, fid: utils.FID(tableName)}
	t.sst = OpenSStable(&model.FileOptions{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    bd.size,
		FID:      t.fid,
	})
	buf := make([]byte, bd.size)
	written := bd.copy(buf)
	common.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	mmapBuf, err := t.sst.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(mmapBuf, buf)
	return t, nil
}

func (ssb *sstBuilder) done() buildData {
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
	bd.size = int(dataSize) + len(blockIndex) + len(checksum) + 4 /* len(blockIndex) */ + 4 /* len(checksum) */
	return bd
}

func (ssb *sstBuilder) buildBlockIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = ssb.keyCount
	tableIndex.MaxVersion = ssb.maxVersion
	tableIndex.Offsets = ssb.writeBlockList()
	var dataBlockSize uint32
	for i := 0; i < len(ssb.blockList); i++ {
		dataBlockSize += uint32(ssb.blockList[i].endOffset)
	}
	marshal, err := tableIndex.Marshal()
	common.Panic(err)
	return marshal, dataBlockSize
}

func (ssb *sstBuilder) writeBlockList() []*pb.BlockOffset {
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
func (ssb *sstBuilder) finishBlock() {
	if ssb.curBlock == nil || len(ssb.curBlock.entryOffsets) == 0 {
		return
	}
	ssb.append(model.U32SliceToBytes(ssb.curBlock.entryOffsets))
	ssb.append(model.U32ToBytes(uint32(len(ssb.curBlock.entryOffsets))))

	// 8B
	checksum := ssb.calculateChecksum(ssb.curBlock.data[:ssb.curBlock.endOffset])

	// Append the block checksum and its length.
	ssb.append(checksum)
	ssb.append(model.U32ToBytes(uint32(len(checksum))))

	ssb.estimateSize += ssb.curBlock.estimateSize
	ssb.blockList = append(ssb.blockList, ssb.curBlock)
	ssb.keyCount += uint32(len(ssb.curBlock.entryOffsets))
	ssb.curBlock = nil
	return
}

func (ssb *sstBuilder) calculateChecksum(data []byte) []byte {
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

func (ssb *sstBuilder) empty() bool {
	return len(ssb.keyHashes) == 0
}

func (ssb *sstBuilder) ReachedCapacity() bool {
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
	it          model.Item
}

func (itr *blockIterator) setBlock(b *block) {
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.prevOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	// 只截取data部分, 剩余的 索引部分 不需要;
	itr.data = b.data[:b.entriesIndexOff]
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
	startIndex := 0
	// 成立的话 往左走, 否则向右走;
	findEntryIndex := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		if idx < startIndex {
			return false
		}
		itr.setIndex(idx)
		// todo block 寻找 key
		return model.CompareKey(itr.key, key) >= 0
	})
	itr.setIndex(findEntryIndex)
}

func (itr *blockIterator) setIndex(idx int) {
	if idx >= len(itr.entryOffsets) || idx < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	itr.idx = idx
	// 找到entry data区域
	startOffset := int(itr.entryOffsets[idx])
	if len(itr.baseKey) == 0 { // 说明当前 block 没有重叠key, 因此直接获得不同的key区间
		var header entryHeader
		header.decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+header.dif]
	}
	var endOffset int
	if idx+1 == len(itr.entryOffsets) {
		endOffset = len(itr.data)
	} else {
		endOffset = int(itr.entryOffsets[itr.idx+1])
	}

	entryData := itr.data[startOffset:endOffset]
	var header entryHeader
	header.decode(entryData)
	// 设置 key 重叠区间
	if header.overlap > itr.prevOverlap {
		itr.key = append(itr.key[0:itr.prevOverlap], itr.baseKey[itr.prevOverlap:header.overlap]...)
	}
	itr.prevOverlap = header.overlap
	valueOffset := headerSize + header.dif
	diffKey := entryData[headerSize:valueOffset]
	itr.key = append(itr.key[:header.overlap], diffKey...)
	eny := &model.Entry{Key: itr.key}
	val := &model.ValueExt{}
	val.DecodeVal(entryData[valueOffset:])
	itr.val = val.Value
	eny.Value = itr.val
	eny.ExpiresAt = val.ExpiresAt
	eny.Meta = val.Meta
	itr.it = model.Item{Item: eny}
}

func (itr *blockIterator) Next() {
	itr.setIndex(itr.idx + 1)
}

func (itr *blockIterator) Valid() bool {
	return itr.err != io.EOF
}

func (itr *blockIterator) Rewind() {
	itr.setIndex(0)
}

func (itr *blockIterator) Item() model.Item {
	return itr.it
}

func (itr *blockIterator) Close() error {
	return nil
}

func (itr *blockIterator) Error() error {
	return itr.err
}
