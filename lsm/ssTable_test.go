package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/interfaces"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/utils"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

var dirPath = "/usr/golanddata/trainkv/sst"

func TestOpenSStable(t *testing.T) {
	tableName := filepath.Join(dirPath, "00001.sst")
	clearDir(dirPath)
	options := GetDefaultOpt(dirPath)
	fid := utils.FID(tableName)
	levelManger := &LevelsManger{}
	levelManger.cache = newLevelsCache(options)
	table := &Table{lm: levelManger, fid: fid, Name: strconv.FormatUint(fid, 10) + SSTableName}
	table.sst = OpenSStable(&utils.FileOptions{
		FileName: tableName,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int32(options.MemTableSize),
		FID:      table.fid,
	})
	table.IncrRef()
	if err := table.sst.Init(); err != nil {
		common.Err(err)
	}

	iterator := table.NewTableIterator(&interfaces.Options{IsAsc: true, IsSetCache: false})
	iterator.Rewind()
	for iterator.Valid() {
		entry := iterator.Item().Item
		fmt.Printf("key=%s, value=%s, meta:%d, version=%d \n",
			model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
		iterator.Next()
	}
}
