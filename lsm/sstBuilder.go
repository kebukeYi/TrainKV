package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/pb"
	"github.com/kebukeYi/TrainKV/utils"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"unsafe"
)

type sstBuilder struct {
	sstSize       int64
	opt           *Options
	blockList     []*block
	curBlock      *block
	keyCount      uint32
	keyHashes     []uint32 // sst 为单位
	maxVersion    int64
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
	arrPtr := (*[headerSize]byte)(unsafe.Pointer(h))
	copy(arrPtr[:], buf[:headerSize])
}

func newSSTBuilderWithSSTableSize(opt *Options, size int64) *sstBuilder {
	size = 2 * size
	return &sstBuilder{
		opt:     opt,
		sstSize: size,
	}
}

func NewSSTBuilder(opt *Options) *sstBuilder {
	return &sstBuilder{
		opt:     opt,
		sstSize: opt.BaseTableSize,
	}
}

func (ssb *sstBuilder) AddKey(e *model.Entry) {
	ssb.Add(e, false)
}

func (ssb *sstBuilder) AddStaleKey(e *model.Entry) {
	ssb.staleDataSize += len(e.Key) + len(e.Value) + 4 /* entry offset */ + 4 /* header size */
	ssb.Add(e, true)
}

func (ssb *sstBuilder) Add(e *model.Entry, isStale bool) {
	key := e.Key
	val := model.ValueExt{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	// 检查是否需要分配一个新的 block;
	if ssb.tryNewBlock(e) {
		if isStale {
			ssb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		ssb.finishBlock()
		ssb.curBlock = &block{
			data: make([]byte, ssb.opt.BlockSize),
		}
	}
	// todo 当前 sst.bloom 中 加入 祛除 Key Ts版本号
	ssb.keyHashes = append(ssb.keyHashes, utils.Hash(model.ParseKey(key)))

	if version := model.ParseTsVersion(key); version > ssb.maxVersion {
		ssb.maxVersion = version
	}

	// baseKey:  key:timestamp
	// 按照 block 为单位 构建 baseKey;
	var diffKey []byte
	if len(ssb.curBlock.baseKey) == 0 {
		ssb.curBlock.baseKey = append(ssb.curBlock.baseKey, key...)
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
	// 记录每一个 kv 的位置, 所有单个entry来构建restart Point[];
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
	if len(ssb.curBlock.entryOffsets) == 0 {
		return false
	}

	sz := uint32((len(ssb.curBlock.entryOffsets)+1)*4 + 4 + 8 + 4)
	common.CondPanic(!(sz < math.MaxUint32),
		errors.New("block size too large,integer overflow!"))

	// (endOffset+1)*4+ len(key)+len(value)
	entriesOffsetsSize := int64((len(ssb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length

	ssb.curBlock.estimateSize = int64(ssb.curBlock.endOffset) + int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(e.EncodeSize()) + entriesOffsetsSize
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

func (ssb *sstBuilder) flush(lm *LevelsManger, tableName string) (t *Table, err error) {
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
	common.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	mmapBuf, err := t.sst.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	// copy 之前 文件建立好了, 但是数据还没复制完毕, 宕机了; 怎么办?
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

func (ssb *sstBuilder) Finish() []byte {
	// 构建 table的数据;
	bd := ssb.done()
	buf := make([]byte, bd.size)
	written := bd.copy(buf)
	common.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	return buf
}

func (ssb *sstBuilder) buildBlockIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = ssb.keyCount
	tableIndex.MaxVersion = uint64(ssb.maxVersion)
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
	// 将当前 block 的元信息 打包进去
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

func (ssb *sstBuilder) close() bool {
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
	startIndex := 0
	// 成立的话 往左走, 否则向右走;
	findEntryIndex := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		if idx < startIndex {
			return false
		}
		itr.setIndex(idx)
		// todo block 寻找 key
		return model.CompareKeyNoTs(itr.key, key) >= 0
	})
	// idx = 0 有可能也是不存在值的;(例如寻找最小不存在的值)
	itr.setIndex(findEntryIndex)
}
func (itr *blockIterator) setIndex(idx int) {
	itr.idx = idx // v2.0
	if idx >= len(itr.entryOffsets) || idx < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	// 找到entry data区域;
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
	// 设置 key 重叠区间;
	if header.overlap > itr.prevOverlap {
		itr.key = append(itr.key[0:itr.prevOverlap], itr.baseKey[itr.prevOverlap:header.overlap]...)
	}
	itr.prevOverlap = header.overlap
	valueOffset := headerSize + header.dif
	diffKey := entryData[headerSize:valueOffset]
	itr.key = append(itr.key[:header.overlap], diffKey...)
	eny := model.Entry{}
	eny.Key = model.SafeCopy(eny.Key, itr.key)
	val := &model.ValueExt{}
	val.DecodeVal(entryData[valueOffset:])
	itr.val = val.Value
	eny.Value = itr.val
	eny.ExpiresAt = val.ExpiresAt
	eny.Meta = val.Meta
	eny.Version = model.ParseTsVersion(itr.key)
	itr.it = model.Item{Item: eny}
}
func (itr *blockIterator) Next() {
	itr.setIndex(itr.idx + 1)
}
func (itr *blockIterator) Prev() {
	itr.setIndex(itr.idx - 1)
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
