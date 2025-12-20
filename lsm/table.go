package lsm

import (
	"encoding/binary"
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/interfaces"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/pb"
	"github.com/kebukeYi/TrainKV/utils"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

const SSTableName string = ".sst"

type Table struct {
	sst  *SSTable
	lm   *LevelsManger
	fid  uint64
	ref  int32
	Name string
}

func OpenTable(lm *LevelsManger, tableName string, builder *sstBuilder) (*Table, error) {
	var (
		t   *Table
		err error
	)
	fid := utils.FID(tableName)

	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			common.Err(err)
			return nil, err
		}
	} else {
		t = &Table{lm: lm, fid: fid, Name: strconv.FormatUint(fid, 10) + SSTableName}
		t.sst = OpenSStable(&utils.FileOptions{
			FileName: tableName,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int32(0),
			FID:      fid,
		})
	}
	t.IncrRef()
	if err = t.sst.Init(); err != nil {
		common.Err(err)
		return nil, err
	}
	// 获取sst的最大key 需要使用迭代器, 逆向获得;
	itr := t.NewTableIterator(&interfaces.Options{IsAsc: false})
	defer itr.Close()
	itr.Rewind()
	common.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey,err:%s", itr.err))

	maxKey := itr.Item().Item.Key
	t.sst.SetMaxKey(maxKey)
	return t, nil
}

func (t *Table) Search(keyTs []byte) (entry model.Entry, err error) {
	t.IncrRef()
	defer t.DecrRef()
	indexData := t.sst.Indexs()
	bloomFilter := utils.Filter(indexData.BloomFilter)
	if t.sst.HasBloomFilter() && !bloomFilter.MayContainKey(model.ParseKey(keyTs)) {
		return model.Entry{Version: 0}, common.ErrKeyNotFound
	}
	iterator := t.NewTableIterator(&interfaces.Options{IsAsc: true})
	defer iterator.Close()
	iterator.Seek(keyTs)
	if !iterator.Valid() {
		return model.Entry{Version: 0}, iterator.err
	}
	// 额外:有可能在迭代器有效的情况下, 返回和keyTs完全不相同的数据key;
	// 因此需要再判断一次;
	if model.SameKeyNoTs(keyTs, iterator.Item().Item.Key) {
		item := iterator.Item().Item
		safeCopy := item.SafeCopy()
		return safeCopy, nil
	}
	return model.Entry{Version: 0}, common.ErrKeyNotFound
}

func (t *Table) MaxVersion() uint64 {
	return t.sst.MaxVersion
}

func (t *Table) getBlock(idx int, IsSetCache bool) (*block, error) {
	if idx >= len(t.sst.Indexs().GetOffsets()) {
		return nil, errors.New("block out of index")
	}
	var b *block
	blockCacheKey := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blockData.Get(blockCacheKey)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}
	var ko pb.BlockOffset
	isGetBlockOffset := t.getBlockOffset(idx, &ko)
	common.CondPanic(!isGetBlockOffset, fmt.Errorf("block t.offset id=%d is ouf of range", idx))
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
	entriesStart := readPos - (numEntries * 4)
	entriesEnd := entriesStart + (numEntries * 4)

	b.entryOffsets = model.BytesToU32Slice(b.data[entriesStart:entriesEnd]) // 5. read entry[]
	b.entriesIndexOff = entriesStart
	if IsSetCache {
		t.lm.cache.blockData.Set(blockCacheKey, b) // 6. cache block
	}
	return b, nil
}

func (t *Table) getBlockOffset(idx int, blo *pb.BlockOffset) bool {
	indexData := t.sst.Indexs()
	if idx < 0 || idx >= len(indexData.GetOffsets()) {
		return false
	}
	blockOffset := indexData.GetOffsets()[idx]
	*blo = *blockOffset
	return true
}

func (t *Table) read(off, sz int) ([]byte, error) {
	return t.sst.Bytes(off, sz)
}

func (t *Table) indexFIDKey() uint64 {
	return t.fid
}

func (t *Table) blockCacheKey(idx int) []byte {
	common.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	common.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))

	return buf
}

func (t *Table) Size() int64 { return t.sst.Size() }

func (t *Table) getStaleDataSize() uint32 {
	return t.sst.Indexs().StaleDataSize
}

func (t *Table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *Table) DecrRef() error {
	atomic.AddInt32(&t.ref, -1)
	if t.ref == 0 {
		// TODO 从缓存中删除
		for i := 0; i < len(t.sst.Indexs().GetOffsets()); i++ {
			t.lm.cache.blockData.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func decrRefs(tables []*Table) error {
	for _, t := range tables {
		if err := t.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) GetCreatedAt() *time.Time {
	return t.sst.GetCreatedAt()
}

func (t *Table) Delete() error {
	//fmt.Printf("delete sstTable:  %d.sst;\n", t.sst.fid)
	return t.sst.Delete()
}

type TableIterator struct {
	name         string
	it           interfaces.Item
	opt          *interfaces.Options
	t            *Table
	blockIterPos int
	biter        *blockIterator
	err          error
}

func (t *Table) NewTableIterator(opt *interfaces.Options) *TableIterator {
	t.IncrRef()
	return &TableIterator{opt: opt, t: t, biter: &blockIterator{}, name: t.Name}
}
func (tier *TableIterator) Name() string {
	return tier.name
}
func (tier *TableIterator) Item() interfaces.Item {
	return tier.biter.it
}
func (tier *TableIterator) Rewind() {
	if tier.opt.IsAsc {
		tier.SeekToFirst()
	} else {
		tier.SeekToLast()
	}
}
func (tier *TableIterator) Next() {
	if tier.opt.IsAsc {
		tier.next()
	} else {
		tier.prev()
	}
}
func (tier *TableIterator) next() {
	tier.err = nil
	if tier.blockIterPos >= len(tier.t.sst.Indexs().GetOffsets()) {
		tier.err = io.EOF
		return
	}

	if len(tier.biter.data) == 0 {
		Block, err := tier.t.getBlock(tier.blockIterPos, tier.opt.IsSetCache)
		if err != nil {
			tier.err = err
			return
		}
		tier.biter.tableID = tier.t.fid
		tier.biter.blockID = tier.blockIterPos
		tier.biter.setBlock(Block)
		tier.biter.seekToFirst()
		tier.err = tier.biter.Error()
		return
	}

	tier.biter.Next()
	if !tier.biter.Valid() { // 当前block已经遍历完了, 换下一个block;
		tier.blockIterPos++
		tier.biter.data = nil
		tier.Next()
		return
	}
	tier.it = tier.biter.Item()
}
func (tier *TableIterator) prev() {
	tier.err = nil
	if tier.blockIterPos < 0 {
		tier.err = io.EOF
		return
	}
	if tier.biter.data == nil {
		block, err := tier.t.getBlock(tier.blockIterPos, tier.opt.IsSetCache)
		if err != nil {
			tier.err = err
			return
		}
		tier.biter.tableID = tier.t.fid
		tier.biter.blockID = tier.blockIterPos
		tier.biter.setBlock(block)
		tier.biter.seekToLast()
		//tier.it = tier.biter.Item()
		tier.err = tier.biter.Error()
		return
	}
	tier.biter.Prev()
	// 无效的话,当前block到头了,说明需要切换到前一个block;
	if !tier.biter.Valid() {
		tier.blockIterPos--
		tier.biter.data = nil
		tier.prev()
		return
	}
}
func (tier *TableIterator) Valid() bool {
	return tier.err == nil // 如果没有的时候 则是EOF;
}

// Seek 在 sst 中扫描 block 索引数据来寻找 合适的 block;
func (tier *TableIterator) Seek(key []byte) {
	if tier.opt.IsAsc {
		tier.seek(key)
	} else {
		tier.seekPrev(key)
	}
}
func (tier *TableIterator) seek(key []byte) {
	tier.seekFrom(key)
}
func (tier *TableIterator) seekPrev(key []byte) {
	tier.seekFrom(key)
	currKey := tier.Item().Item.Key
	if !model.SameKeyNoTs(key, currKey) {
		tier.prev()
	}
}

func (tier *TableIterator) seekFrom(key []byte) {
	var blo pb.BlockOffset
	blockOffsetLen := len(tier.t.sst.Indexs().GetOffsets())
	idx := sort.Search(blockOffsetLen, func(index int) bool {
		getBlockOffset := tier.t.getBlockOffset(index, &blo)
		common.CondPanic(!getBlockOffset, fmt.Errorf("TableIterator.sort.Search idx < 0 || idx > len(index.GetOffsets()"))
		if index == blockOffsetLen {
			return true
		}
		blockBaseKey := blo.GetKey() // block.baseKey, 每个block中的第一个key;
		compareKeyNoTs := model.CompareKeyWithTs(blockBaseKey, key)
		return compareKeyNoTs > 0
	})

	// todo table 寻找相关 block;
	if idx == 0 { // 说明库中没有这个key; 但是依然选择返回库中最小的值;
		tier.SeekBlock(0, key)
		return
	}
	// 没有找到大于key的block,因此返回n,返回库中存在的最大值;
	// idx in [0,n) 区间, 那就取前一个 block 进行查询;
	tier.SeekBlock(idx-1, key)
}
func (tier *TableIterator) SeekBlock(blockIdx int, key []byte) {
	tier.blockIterPos = blockIdx
	block, err := tier.t.getBlock(blockIdx, tier.opt.IsSetCache)
	if err != nil {
		tier.err = err
		return
	}
	tier.biter.tableID = tier.t.fid
	tier.biter.blockID = tier.blockIterPos
	tier.biter.setBlock(block)
	// 从 block 中 加载 entry;
	tier.biter.Seek(key)
	tier.err = tier.biter.Error()
}

func (tier *TableIterator) SeekToFirst() {
	numsBlocks := len(tier.t.sst.Indexs().GetOffsets())
	if numsBlocks == 0 {
		tier.err = io.EOF
		return
	}
	tier.blockIterPos = 0
	var Block *block
	var err error
	if Block, err = tier.t.getBlock(tier.blockIterPos, tier.opt.IsSetCache); err != nil {
		tier.err = err
		return
	}
	tier.biter.blockID = tier.blockIterPos
	tier.biter.tableID = tier.t.fid
	tier.biter.setBlock(Block)
	tier.biter.seekToFirst()
	tier.err = tier.biter.Error()
}
func (tier *TableIterator) SeekToLast() {
	numsBlocks := len(tier.t.sst.Indexs().GetOffsets())
	if numsBlocks == 0 {
		tier.err = io.EOF
		return
	}
	tier.blockIterPos = numsBlocks - 1
	var Block *block
	var err error
	if Block, err = tier.t.getBlock(tier.blockIterPos, tier.opt.IsSetCache); err != nil {
		tier.err = err
		return
	}
	tier.biter.blockID = tier.blockIterPos
	tier.biter.tableID = tier.t.fid
	tier.biter.setBlock(Block)
	tier.biter.seekToLast()
	tier.err = tier.biter.Error()
}
func (tier *TableIterator) Close() error {
	tier.biter.Close()
	return tier.t.DecrRef()
}
