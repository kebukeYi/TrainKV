package common

import (
	"hash/crc32"
	"os"
)

const (
	// MaxLevelNum _
	MaxLevelNum = 7
	// DefaultValueThreshold _
	DefaultValueThreshold = 1024
)

// file
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	ManifestFileHeaderLen             = 8
	ManifestFileCrcLen                = 8
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666
	MaxValueLogSize                   = 10 << 20
	MaxHeaderSize                     = 21 // 基于可变长编码,vlogFile其最可能的编码
	VlogHeaderSize                    = 0
	KVWriteChCapacity                 = 1000
)

// meta
const (
	BitDelete       byte = 1 << 0 // Set if the key has been deleted.
	BitValuePointer byte = 1 << 1 // Set if the value is NOT stored directly next to key.
)

var (
	MagicText    = [4]byte{'H', 'A', 'R', 'D'} // Manifest 文件的头8B中的前4B魔数 Magic
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
