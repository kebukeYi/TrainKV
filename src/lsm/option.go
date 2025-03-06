package lsm

import (
	"trainKv/skl"
)

type Options struct {
	WorkDir      string // 工作数据目录
	MemTableSize int64  // 内存表最大限制
	SSTableMaxSz int64  // SSSTable 最大限制
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize uint32 // 数据持久化时的大小
	// BloomFalsePositive is the false positive probability of bloom filter.
	BloomFalsePositive float64 // 布隆过滤器的容错率;

	CacheNums int // 缓存元素个数, 缺省值默认 1024*10个;

	ValueThreshold      int
	ValueLogMaxEntries  int32
	ValueLogFileSize    int32
	VerifyValueChecksum bool

	MaxBatchCount int64
	MaxBatchSize  int64

	// compact 合并相关
	NumCompactors       int   // 合并协程数量;默认1
	BaseLevelSize       int64 // 基层中 所期望的文件大小
	LevelSizeMultiplier int   // 决定 level 之间期望 总体文件 size 比例, 比如是 10倍
	TableSizeMultiplier int   // 决定每层 文件 递增倍数
	BaseTableSize       int64 // 基层中 文件所期望的文件大小
	NumLevelZeroTables  int   // 第 0 层中,允许的表数量
	MaxLevelNum         int   // 最大层数,默认是 7 层

	DiscardStatsCh *chan map[uint32]int64 //  用于 compact 组件 向 vlog 组件 传递信息使用,在合并过程中,知道哪些文件是失效的,让vlog组件知道,方便其GC;
}

const maxValueThreshold = 1 << 20

func GetLSMDefaultOpt(dirPath string) Options {
	return Options{
		WorkDir:             dirPath,
		MemTableSize:        64 << 20,
		BaseTableSize:       2 << 20,
		BaseLevelSize:       10 << 20,
		SSTableMaxSz:        64 << 20,
		LevelSizeMultiplier: 10,
		TableSizeMultiplier: 2,
		MaxLevelNum:         7,
		NumCompactors:       4, // Run at least 2 compactors. Zero-th compactor prioritizes L0.
		NumLevelZeroTables:  5,

		BloomFalsePositive: 0.01,
		BlockSize:          4 * 1024,
		CacheNums:          0,

		ValueThreshold:     maxValueThreshold,
		ValueLogMaxEntries: 1000000,
		// (2^30 - 1)*2 when mmapping < 2^31 - 1, max int32.
		// -1 so 2*ValueLogFileSize won't overflow on 32-bit systems.
		ValueLogFileSize: 1<<30 - 1,

		VerifyValueChecksum: false,
		MaxBatchCount:       0,
		MaxBatchSize:        0,

		DiscardStatsCh: nil,
	}
}

func CheckLSMDefaultOpt(opt *Options) error {
	opt.MaxBatchSize = (15 * opt.MemTableSize) / 100
	opt.MaxBatchCount = opt.MaxBatchSize / int64(skl.MaxSkipNodeSize)
	return nil
}
