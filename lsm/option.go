package lsm

import (
	"github.com/kebukeYi/TrainKV/common"
	"math"
	"os"
)

type Options struct {
	WorkDir string // 工作数据目录;
	// MemTable
	MemTableSize       int64 // 内存表最大限制/字节; 默认和 SSTable 文件大小一致;
	WaitFlushMemTables int   // 刷盘队列, 最大等待数量/个;

	// SSTable
	BlockSize uint32 // 数据块, 持久化大小/字节;

	// BloomFilter
	BloomFalsePositive float64 // 布隆过滤器的容错率/0.01;

	// Cache
	CacheNums int // 缓存元素个数, 缺省值默认 1024*10个;

	// vlogFile
	ValueThreshold      int64  // 进入vlog的value阈值/字节;
	ValueLogMaxEntries  int32  // vlogFile文件保存的entry最大数量;
	ValueLogFileSize    int32  // vlogFile的文件大小;
	ValueLogFileMaxSize uint32 // vlogFile的文件最大尺度;默认 math.MaxUint32;
	SyncWrites          bool

	VerifyValueChecksum bool // 是否开启vlogFile的crc检查;

	// Batch
	MaxBatchCount int64 // 批处理entry数量; 1.  2.txn 提交, 会进行批处理数量判断;
	MaxBatchSize  int64 // 批处理entry总量大小; 1. 2.txn 提交,会进行批处理数量判断;

	// Compact
	NumCompactors       int         // 合并协程数量;默认2;
	BaseLevelSize       int64       // 基层中 所期望的文件大小;
	LevelSizeMultiplier int         // 决定 level 之间期望 总体文件 size 比例, 默认是 10倍;
	TableSizeMultiplier int         // 决定每层 文件 递增倍数;
	BaseTableSize       int64       // 基层中 文件所期望的文件大小;
	NumLevelZeroTables  int         // 第 0 层中, 允许存在的表数量;
	MaxLevelNum         int         // 最大层数,默认是 7 层;
	SSTKeyRangCheckNums int         // 默认是 5000;
	NumVersionsToKeep   int         // 相同key, 可存在的不同版本数量;
	TxnDoneIndexCh      chan uint64 // txn 提交完成, 通知 compact 组件;

	// vlogFile GC; 在合并过程中, 统计出哪些 vlog 文件达到失效阈值比, 通知 vlog 组件, 方便其 vlogFile-GC;
	DiscardStatsCh *chan map[uint32]int64 // 用于 compact 组件向 vlog 组件传递信息;

	// Txn
	DetectConflicts bool // 是否开启事务冲突检查;
}

const KvWriteChCapacity = 1000

const maxValueThreshold = 1 << 20

func GetDefaultOpt(dirPath string) *Options {
	return &Options{
		WorkDir: dirPath,
		// MemTable
		MemTableSize:       64 << 20,
		WaitFlushMemTables: 6,
		// SSTable
		BlockSize: 4 * 1024,
		// BloomFilter
		BloomFalsePositive: 0.01,
		// Cache
		CacheNums: 1024 * 10,
		// vlogFile
		ValueThreshold:      maxValueThreshold,
		ValueLogMaxEntries:  1000,
		ValueLogFileSize:    1<<30 - 1,
		ValueLogFileMaxSize: math.MaxUint32,
		SyncWrites:          false,
		VerifyValueChecksum: false,
		// vlogGC
		DiscardStatsCh: nil,
		// Batch
		MaxBatchCount: 1000,
		MaxBatchSize:  10 << 20,
		// Compact
		NumCompactors:       4,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		TableSizeMultiplier: 2,
		BaseTableSize:       2 << 20,
		NumLevelZeroTables:  5,
		MaxLevelNum:         common.MaxLevelNum,
		SSTKeyRangCheckNums: 5000,
		NumVersionsToKeep:   1,
		TxnDoneIndexCh:      nil,
		// Txn
		DetectConflicts: true,
	}
}

func CheckOpt(opt *Options) (func() error, error) {
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
