package lsm

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
	"trainKv/common"
	"trainKv/model"
	"trainKv/pb"
	"trainKv/utils"
)

const SSTableName string = ".sst"

type table struct {
	sst  *SSTable
	lm   *levelsManger
	fid  uint64
	ref  int32
	Name string
}

func openTable(lm *levelsManger, tableName string, builder *sstBuilder) *table {
	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			common.Err(err)
			return nil
		}
	} else {
		t = &table{lm: lm, fid: fid, Name: strconv.FormatUint(fid, 10) + SSTableName}
		t.sst = OpenSStable(&model.FileOptions{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(lm.opt.SSTableMaxSz),
			FID:      fid,
		})
	}
	t.IncrRef()
	if err = t.sst.Init(); err != nil {
		common.Err(err)
		return nil
	}
	// 获取sst的最大key 需要使用迭代器
	itr := t.NewTableIterator(&model.Options{IsAsc: false})
	defer itr.Close()
	itr.Rewind()
	common.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))

	maxKey := itr.Item().Item.Key
	t.sst.SetMaxKey(maxKey)
	return t
}

func (t *table) Search(key []byte, maxVs *uint64) (entry *model.Entry, err error) {
	t.IncrRef()
	defer t.DecrRef()
	indexData := t.sst.Indexs()
	bloomFilter := utils.Filter(indexData.BloomFilter)
	if t.sst.HasBloomFilter() && !bloomFilter.MayContainKey(model.ParseKey(key)) {
		return nil, common.ErrNotFound
	}
	iterator := t.NewTableIterator(&model.Options{IsAsc: true})
	defer iterator.Close()
	iterator.Seek(key)
	if !iterator.Valid() {
		return nil, common.ErrKeyNotFound
	}
	// todo 2. 判断key是否在sst中,也是祛除掉了 Key 的Ts版本号
	if model.SameKey(key, iterator.Item().Item.Key) {
		//if version := model.ParseTsVersion(iterator.Item().Item.Key); *maxVs < version {
		if version := model.ParseTsVersion(iterator.Item().Item.Key); version > 0 {
			*maxVs = version
			return iterator.Item().Item, nil
		}
	}
	return nil, common.ErrKeyNotFound
}

func (t *table) getBlock(idx int) (*block, error) {
	if idx >= len(t.sst.Indexs().GetOffsets()) {
		return nil, errors.New("block out of index")
	}
	var b *block
	blockCacheKey := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.Get(string(blockCacheKey))
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}
	var ko pb.BlockOffset
	getBlockOffset := t.getBlockOffset(idx, &ko)
	common.CondPanic(!getBlockOffset, fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(ko.Offset),
	}
	var err error
	if b.data, err = t.read(b.offset, int(ko.GetSize_())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.sst.FID(), b.offset, ko.GetSize_())
	}
	readPos := len(b.data) - 4 // 1. First read checksum length.
	b.chkLen = int(binary.BigEndian.Uint32(b.data[readPos : readPos+4]))
	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}
	readPos -= b.chkLen
	b.checkSum = b.data[readPos : readPos+b.chkLen] // 2. read checkSum bytes
	b.data = b.data[:readPos]                       // 3. read data
	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	// restart point len
	readPos -= 4
	numEntries := int(binary.BigEndian.Uint32(b.data[readPos : readPos+4])) // 4. read numEntries
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + (numEntries * 4)

	b.entryOffsets = model.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd]) // 5. read entry[]
	b.entriesIndexOff = entriesIndexStart
	t.lm.cache.Set(string(blockCacheKey), b) // 6. cache block
	return b, nil
}

func (t *table) getBlockOffset(idx int, blo *pb.BlockOffset) bool {
	indexData := t.sst.Indexs()
	if idx < 0 || idx >= len(indexData.GetOffsets()) {
		return false
	}
	if idx == len(indexData.GetOffsets()) {
		return true
	}
	blockOffset := indexData.GetOffsets()[idx]
	*blo = *blockOffset
	return true
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.sst.Bytes(off, sz)
}

func (t *table) indexFIDKey() uint64 {
	return t.fid
}

func (t *table) blockCacheKey(idx int) []byte {
	common.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	common.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))

	return buf
}

func (t *table) Size() int64 { return t.sst.Size() }

func (t *table) getStaleDataSize() uint32 {
	return t.sst.Indexs().StaleDataSize
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}
func (t *table) DecrRef() error {
	atomic.AddInt32(&t.ref, -1)
	return nil
}
func decrRefs(tables []*table) error {
	for _, t := range tables {
		if err := t.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}
func (t *table) GetCreatedAt() *time.Time {
	return t.sst.GetCreatedAt()
}
func (t *table) Delete() error {
	return t.sst.Detele()
}

// table 层面的容器迭代器
type tableIterator struct {
	name     string
	it       model.Item
	opt      *model.Options
	t        *table
	blockPos int
	biter    *blockIterator
	err      error
}

func (t *table) NewTableIterator(opt *model.Options) *tableIterator {
	t.IncrRef()
	return &tableIterator{opt: opt, t: t, biter: &blockIterator{}, name: t.Name}
}
func (tier *tableIterator) Name() string {
	return tier.name
}

func (tier *tableIterator) Item() model.Item {
	return tier.it
}

func (tier *tableIterator) Rewind() {
	if tier.opt.IsAsc {
		tier.SeekToFirst()
	} else {
		tier.SeekToLast()
	}
}

func (tier *tableIterator) Next() {
	tier.err = nil
	if tier.blockPos >= len(tier.t.sst.Indexs().GetOffsets()) {
		tier.err = io.EOF
		return
	}

	if len(tier.biter.data) == 0 {
		Block, err := tier.t.getBlock(tier.blockPos)
		if err != nil {
			tier.err = err
			return
		}
		tier.biter.tableID = tier.t.fid
		tier.biter.blockID = tier.blockPos
		tier.biter.setBlock(Block)
		tier.biter.seekToFirst()
		tier.it = tier.biter.Item()
		tier.err = tier.biter.Error()
		return
	}

	tier.biter.Next()
	if !tier.biter.Valid() { // 当前block已经遍历完了, 换下一个block
		tier.blockPos++
		tier.biter.data = nil
		tier.Next()
		return
	}
	// todo 这是何意? badger 源码中没有;
	tier.it = tier.biter.Item()
}

func (tier *tableIterator) Valid() bool {
	return tier.err != io.EOF // 如果没有的时候 则是EOF
}

// Seek 在 sst 中扫描 index 索引数据来寻找 合适的 block
func (tier *tableIterator) Seek(key []byte) {
	var blo pb.BlockOffset
	blockOffsetLen := len(tier.t.sst.Indexs().GetOffsets())
	idx := sort.Search(blockOffsetLen, func(index int) bool {
		getBlockOffset := tier.t.getBlockOffset(index, &blo)
		common.CondPanic(!getBlockOffset,
			fmt.Errorf("tableIterator.sort.Search idx < 0 || idx > len(index.GetOffsets()"))
		if index == blockOffsetLen {
			return true
		}
		//return model.CompareKey(blo.GetKey(), key) > 0
		return model.CompareBaseKeyNoTs(blo.GetKey(), key) > 0
	})
	// todo table 寻找key
	if idx == 0 { // 应该说明 没有这个 key 啊,找不到此key啊;
		// 这个 sst 中没有这个 key
		tier.SeekHelper(0, key)
		return
	}
	// 1. 没有找到,返回n
	// 2. 找到了,找到了也返回n
	// 只能碰碰运气
	//if idx == blockOffsetLen {
	//	tier.SeekHelper(idx, key)
	//	return
	//}
	// idx in (0,n) 区间, 那就取前一个 block 进行查询;
	tier.SeekHelper(idx-1, key)
}

// SeekHelper 在 sst 中指定 block 进行扫描
func (tier *tableIterator) SeekHelper(blockIdx int, key []byte) {
	tier.blockPos = blockIdx
	// 加载 block
	block, err := tier.t.getBlock(blockIdx)
	if err != nil {
		tier.err = err
		return
	}
	tier.biter.tableID = tier.t.fid
	tier.biter.blockID = tier.blockPos
	tier.biter.setBlock(block)
	// 从 block 中 加载 entry
	tier.biter.Seek(key)
	tier.it = tier.biter.Item()
	tier.err = tier.biter.Error()
}

func (tier *tableIterator) SeekToFirst() {
	numsBlocks := len(tier.t.sst.Indexs().GetOffsets())
	if numsBlocks == 0 {
		tier.err = io.EOF
		return
	}
	tier.blockPos = 0
	var Block *block
	var err error
	if Block, err = tier.t.getBlock(tier.blockPos); err != nil {
		tier.err = err
		return
	}
	tier.biter.blockID = tier.blockPos
	tier.biter.tableID = tier.t.fid
	tier.biter.setBlock(Block)
	tier.biter.seekToFirst()
	tier.it = tier.biter.Item()
	tier.err = tier.biter.Error()
}
func (tier *tableIterator) SeekToLast() {
	numsBlocks := len(tier.t.sst.Indexs().GetOffsets())
	if numsBlocks == 0 {
		tier.err = io.EOF
		return
	}
	tier.blockPos = numsBlocks - 1
	var Block *block
	var err error
	if Block, err = tier.t.getBlock(tier.blockPos); err != nil {
		tier.err = err
		return
	}
	tier.biter.blockID = tier.blockPos
	tier.biter.tableID = tier.t.fid
	tier.biter.setBlock(Block)
	tier.biter.seekToLast()
	tier.it = tier.biter.Item()
	tier.err = tier.biter.Error()
}

func (tier *tableIterator) Close() error {
	tier.biter.Close()
	return tier.t.DecrRef()
}
