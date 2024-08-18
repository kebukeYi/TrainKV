package utils

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"trainKv/common"
)

// LoadIDMap Get the id of all sst files in the current folder
func LoadIDMap(dir string) map[uint64]struct{} {
	fileInfos, err := os.ReadDir(dir)
	common.Err(err)
	idMap := make(map[uint64]struct{})
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		fileID := FID(info.Name())
		if fileID != 0 {
			idMap[fileID] = struct{}{}
		}
	}
	return idMap
}

// FID 根据file name 获取其fid
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		common.Panic(err)
		return 0
	}
	return uint64(id)
}
func VlogFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%05d.vlog", dirPath, string(os.PathSeparator), fid)
}

func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

// VerifyChecksum crc32
func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, common.CastagnoliCrcTable))
	expectedU64 := binary.BigEndian.Uint64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(common.ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}

	return nil
}

// CalculateChecksum _
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, common.CastagnoliCrcTable))
}

func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}
