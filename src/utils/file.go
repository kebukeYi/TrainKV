package utils

import (
	"github.com/pkg/errors"
	"hash/crc32"
	"path"
	"strconv"
	"strings"
	"trainKv/common"
	"trainKv/model"
)

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

// VerifyChecksum crc32
func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, common.CastagnoliCrcTable))
	expectedU64 := model.BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(common.ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}

	return nil
}

// CalculateChecksum _
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, common.CastagnoliCrcTable))
}
