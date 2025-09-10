package lsm

import (
	"github.com/kebukeYi/TrainKV/common"
	"os"
)

type Options struct {
	WorkDir      string // 工作数据目录;
	MemTableSize int64  // 内存表最大限制;
	// SSTableMaxSz      int64  // SSSTable 最大限制,同上;
	NumFlushMemtables int    // 刷盘队列大小;
	BlockSize         uint32 // 数据块持久化时的大小;

	BloomFalsePositive float64 // 布隆过滤器的容错率;

	CacheNums int // 缓存元素个数, 缺省值默认 1024*10个;

	ValueThreshold      int   // 进入vlog的value阈值;
	ValueLogMaxEntries  int32 // vlogFile文件保存的entry最大数量;
	ValueLogFileSize    int32 // vlogFile的文件大小;
	VerifyValueChecksum bool  // 是否开启vlogFile的crc检查;

	MaxBatchCount int64 // 批处理entry数量;
	MaxBatchSize  int64 // 批处理entry总量大小;

	// compact 合并相关
	NumCompactors       int   // 合并协程数量;默认2;
	BaseLevelSize       int64 // 基层中 所期望的文件大小;
	LevelSizeMultiplier int   // 决定 level 之间期望 总体文件 size 比例, 默认是 10倍;
	TableSizeMultiplier int   // 决定每层 文件 递增倍数;
	BaseTableSize       int64 // 基层中 文件所期望的文件大小;
	NumLevelZeroTables  int   // 第 0 层中,允许的表数量;
	MaxLevelNum         int   // 最大层数,默认是 7 层;

	DiscardStatsCh *chan map[uint32]int64 //  用于 compact 组件向 vlog 组件传递信息使用,在合并过程中,知道哪些文件是失效的,让vlog组件知道,方便其GC;
}

const KvWriteChCapacity = 1000

const maxValueThreshold = 1 << 20

func GetLSMDefaultOpt(dirPath string) *Options {
	return &Options{
		WorkDir:             dirPath,
		MemTableSize:        64 << 20,
		NumFlushMemtables:   6,
		BaseTableSize:       2 << 20,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		TableSizeMultiplier: 2,
		MaxLevelNum:         common.MaxLevelNum,
		NumCompactors:       4,
		NumLevelZeroTables:  5,

		BloomFalsePositive: 0.01,
		BlockSize:          4 * 1024,
		CacheNums:          0,

		ValueThreshold:     maxValueThreshold,
		ValueLogMaxEntries: 1000,

		ValueLogFileSize: 1<<30 - 1,

		VerifyValueChecksum: false,

		MaxBatchCount: 1000,
		MaxBatchSize:  10 << 20,

		DiscardStatsCh: nil,
	}
}

func CheckLSMOpt(opt *Options) (func() error, error) {
	var err error
	var tempDir string
	if opt.WorkDir == "" {
		tempDir, err = os.MkdirTemp("", "trainDB")
		if err != nil {
			panic(err)
		}
		opt.WorkDir = tempDir
	}
	return func() error {
		if tempDir != "" {
			return os.RemoveAll(tempDir)
		}
		return nil
	}, err
}
