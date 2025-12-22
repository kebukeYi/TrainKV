package common

import (
	"hash/crc32"
	"os"
)

const (
	MaxLevelNum                       = 7
	VlogFileDiscardStatsKey           = "VlogFileDiscard" // For storing lfDiscardStats
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	TxnKey                            = "!train!txn!fin"
	LockFile                          = "LOCKFILE"
	ManifestDeletionsRewriteThreshold = 10000 // 次
	MaxKeySize                        = 65000 // B
	ManifestDeletionsRatio            = 10
	ManifestFileHeaderLen             = 8
	ManifestFileCrcLen                = 8
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666 // 666
	MaxHeaderSize                     = 21   // 基于可变长编码,vlogFile其最可能的编码;
	VlogHeaderSize                    = 0
	KVWriteChRequestCapacity          = 1000
)

const LevelMaxStaleDataSize = 10 << 20 // 10MB
const MaxAllocatorInitialSize = 256 << 20

// entry meta
const (
	BitDelete       byte = 1 << 0 //1 set if the key has been deleted.
	BitValuePointer byte = 1 << 2 //2 set if the value is NOT stored directly next to key.
	BitTxn          byte = 1 << 3
	BitFinTxn       byte = 1 << 4
)

var (
	MagicText           = [4]byte{'M', 'A', 'G', 'C'} // Manifest 文件的头8B中的前4B魔数 Magic
	MagicVersion        = uint32(1)
	CastigationCryTable = crc32.MakeTable(crc32.Castagnoli)
)
