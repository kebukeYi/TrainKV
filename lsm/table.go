package lsm

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/pb"
	"github.com/kebukeYi/TrainKV/v2/skl"
	"github.com/kebukeYi/TrainKV/v2/utils"
	pkg_err "github.com/pkg/errors"
)

const SSTableName string = ".sst"

type Table struct {
	sst  *SSTable
	lm   *LevelsManger
	fid  uint64
	ref  int32
	Name string
}

func OpenTable(lm *LevelsManger, tableName string, builder *SstBuilder) (*Table, error) {
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
	itr := t.NewTableIterator(&interfaces.Options{IsAsc: false, IsSetCache: false})
	defer itr.Close()
	itr.Rewind()
	common.CondPanic(!itr.Valid(), pkg_err.Errorf("failed to read index, form maxKey,err:%s", itr.err))

	maxKey := itr.Item().Item.Key
	t.sst.SetMaxKey(maxKey)
	return t, nil
}

// blockKeyPool 复用块内二分探测的 key 拼装缓冲区, 避免每次 Search 重新增长分配;
var blockKeyPool = sync.Pool{New: func() any { return make([]byte, 0, 64) }}

// searchBlock 在指定块内二分查找 keyTs, 返回其 item (命中) 或 not-found;
// setIndex 已把 Key 拷贝为独立内存, Value 仍指向共享的缓存块, 由调用方决定是否拷贝;
func (t *Table) searchBlock(blockIdx int, offsets []*pb.BlockOffset, keyTs []byte) (model.Entry, error) {
	b, err := t.getBlock(blockIdx, true)
	if err != nil {
		return model.Entry{Version: 0}, err
	}
	var bi blockIterator
	bi.key = blockKeyPool.Get().([]byte)
	bi.setBlock(b)
	bi.Seek(keyTs)
	blockKeyPool.Put(bi.key)
	if !bi.Valid() {
		return model.Entry{Version: 0}, bi.err
	}
	item := bi.Item().Item
	if model.SameKeyNoTs(keyTs, item.Key) {
		// Key 已被 setIndex 拷贝; 仅需把 Value 从共享缓存块中拷出;
		item.Value = model.SafeCopy(nil, item.Value)
		return item, nil
	}
	return model.Entry{Version: 0}, common.ErrKeyNotFound
}

func (t *Table) Search(keyTs []byte) (entry model.Entry, err error) {
	t.IncrRef()
	defer t.DecrRef()
	indexData := t.sst.Indexs()
	bloomFilter := utils.Filter(indexData.BloomFilter)
	if t.sst.HasBloomFilter() && !bloomFilter.MayContainKey(model.ParseKey(keyTs)) {
		return model.Entry{Version: 0}, common.ErrKeyNotFound
	}
	// 1. 在 block 索引上二分: 找到第一个 baseKey > keyTs 的 block;
	//    定位逻辑与 TableIterator.seekFrom 一致, 但不需要构造迭代器;
	offsets := indexData.GetOffsets()
	lo, hi := 0, len(offsets)
	for lo < hi {
		mid := (lo + hi) / 2
		if model.CompareKeyWithTs(offsets[mid].GetKey(), keyTs) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	blockIdx := lo - 1
	if blockIdx < 0 {
		// table 中最小的 key 都大于 keyTs, 仍取第一个 block 检查;
		blockIdx = 0
	}
	entry, err = t.searchBlock(blockIdx, offsets, keyTs)
	if !errors.Is(err, common.ErrBlockEOF) {
		return entry, err
	}
	// 目标 key+ts 可能排序在自身版本所在块的前一块末尾(块边界恰好把同名 key 的
	// 各版本切开, 且目标版本排序在前), 此时需回退到下一个块; 与 seekFrom 的
	// ErrBlockEOF 回退逻辑一致;
	if blockIdx+1 >= len(offsets) {
		return model.Entry{Version: 0}, common.ErrBlockEOF
	}
	return t.searchBlock(blockIdx+1, offsets, keyTs)
}

func (t *Table) MaxVersion() uint64 {
	return t.sst.MaxVersion
}

func (t *Table) getBlock(idx int, IsSetCache bool) (*block, error) {
	if idx >= len(t.sst.Indexs().GetOffsets()) {
		return nil, common.ErrBlockEOF
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
	if !isGetBlockOffset {
		return nil, common.ErrBlockEOF
	}
	b = &block{
		offset: int(ko.Offset),
	}
	var err error
	if b.data, err = t.read(b.offset, int(ko.GetSize_())); err != nil {
		return nil, pkg_err.Wrapf(err,
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

func (t *Table) blockCacheKey(idx int) uint64 {
	common.CondPanicf(t.fid >= math.MaxUint32, "t.fid >= math.MaxUint32")
	common.CondPanicf(uint32(idx) >= math.MaxUint32, "uint32(idx) >=  math.MaxUint32")
	return uint64(t.fid)<<32 | uint64(idx)
}

func (t *Table) Size() int64 { return t.sst.Size() }

func (t *Table) getStaleDataSize() uint32 {
	return t.sst.Indexs().StaleDataSize
}

func (t *Table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *Table) DecrRef() error {
	// 用 AddInt32 的返回值判断归零, 不能事后普通读 t.ref:
	// 多个协程 (如 split 并行 compaction) 会并发 DecrRef, 普通读与原子写竞争;
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// TODO 从缓存中删除自己的数据块;
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
	// fmt.Printf("delete sstTable:  %d.sst;\n", t.sst.fid)
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
	// Item key 的稳定副本走分块 arena, 避免每 key 一次堆分配 (nil 时 blockIterator 退回 SafeCopy);
	return &TableIterator{opt: opt, t: t, biter: &blockIterator{arena: skl.NewChunkedArena(64 << 10)}, name: t.Name}
}
func (tier *TableIterator) Name() string {
	return tier.name
}
func (tier *TableIterator) Item() interfaces.Item {
	return tier.biter.it
}
func (tier *TableIterator) Rewind() {
	// 借用契约: Rewind 前所有 Item 均已消费, 旧 key 副本失效, arena 重置以回收内存;
	tier.biter.arena = nil
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
		tier.err = common.ErrBlockEOF
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
		tier.err = common.ErrBlockEOF
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
	return tier.err == nil // 如果不为空的话, 大概率是则是 common.ErrBlockEOF;
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
	offsets := tier.t.sst.Indexs().GetOffsets()
	blockOffsetLen := len(offsets)
	// 手写二分: 找到第一个 baseKey > key 的 block, 等价于 sort.Search 但避免闭包与每次探测的防御性分配;
	lo, hi := 0, blockOffsetLen
	for lo < hi {
		mid := (lo + hi) / 2
		blockBaseKey := offsets[mid].GetKey() // block.baseKey, 每个block中的第一个key(最小键);
		if model.CompareKeyWithTs(blockBaseKey, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo

	// todo table 寻找相关 block;
	if idx == 0 { // 说明当前table中最小的key都大于要找的值,间接说明当前table没有这个key;
		// 但是依然选择返回table中最小的值;
		tier.SeekBlock(0, key)
		return
	}
	// idx in (0,n] 区间, 情况再分析:
	// 情况1:idx in (0,n), 找到了大于key的block,因此返回n-1, 返回前一个block, 试图寻找(有可能依然没有);
	// 情况2:idx=n, 那就说明没有找到大于key的block,因此返回n,返回库中存在的最大值;
	tier.SeekBlock(idx-1, key)
	if pkg_err.Is(tier.err, common.ErrBlockEOF) {
		// 如果此时的 idx 等于 len(), 那么idx-1 就是最后一个block;
		if blockOffsetLen == idx {
			// 最后一个都还是没有找到的话, 那就没有了;
			return
		}
		// table的搜寻宗旨就是 必须要返回一个值: 第一个大于等于 key的值;
		// 在 idx-1没有找到, 那就返回 idx 的值, 来顶顶;
		tier.SeekBlock(idx, key)
	}
}
func (tier *TableIterator) SeekBlock(blockIdx int, key []byte) {
	tier.blockIterPos = blockIdx
	// 获取 block; 超过区间则返回 common.ErrBlockEOF;
	block, err := tier.t.getBlock(blockIdx, tier.opt.IsSetCache)
	if err != nil {
		tier.err = err
		return
	}
	tier.biter.tableID = tier.t.fid
	tier.biter.blockID = tier.blockIterPos
	tier.biter.setBlock(block)
	// 从 block 中 加载 entry; 超过区间 返回 common.ErrBlockEOF;
	tier.biter.Seek(key)
	tier.err = tier.biter.Error()
}

func (tier *TableIterator) SeekToFirst() {
	numsBlocks := len(tier.t.sst.Indexs().GetOffsets())
	if numsBlocks == 0 {
		tier.err = common.ErrBlockEOF
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
		tier.err = common.ErrBlockEOF
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
	err := tier.biter.Close()
	common.Panic(err)
	return tier.t.DecrRef()
}
