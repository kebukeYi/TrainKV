package utils

import (
	"encoding/binary"
	"fmt"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/pkg/errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type FileOptions struct {
	FID      uint64
	FileName string
	Dir      string
	Path     string
	Flag     int
	MaxSz    int32
}

func LoadIDMap(dir string) map[uint64]struct{} {
	fileInfos, err := os.ReadDir(dir)
	common.Err(err)
	idMap := make(map[uint64]struct{})
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		fileID := FID(info.Name())
		if fileID > 0 {
			idMap[fileID] = struct{}{}
		}
	}
	return idMap
}

// FID 根据file name 获取其fid;
func FID(name string) uint64 {
	//name = path.Base(name) // 仅仅处理 /
	name = filepath.Base(name) // 处理 \ 和 /
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

func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, common.CastigationCryTable))
	expectedU64 := binary.BigEndian.Uint64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(common.ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}
	return nil
}
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, common.CastigationCryTable))
}
