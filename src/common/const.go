package common

import (
	"hash/crc32"
	"os"
)

const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestFileHeaderLen             = 8
	ManifestFileCrcLen                = 8
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666
)

var (
	MagicText    = [4]byte{'H', 'A', 'R', 'D'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
