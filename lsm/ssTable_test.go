package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainKV/common"
	"github.com/kebukeYi/TrainKV/model"
	"github.com/kebukeYi/TrainKV/utils"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

var dirPath = "/usr/golanddata/trainkv/sst"

func TestOpenSStable(t *testing.T) {
	tableName := filepath.Join(dirPath, "00001.sst")
	options := GetLSMDefaultOpt("")
	fid := utils.FID(tableName)
	levelManger := &LevelsManger{}
	levelManger.cache = newLevelsCache(options)
	table := &Table{lm: levelManger, fid: fid, Name: strconv.FormatUint(fid, 10) + SSTableName}
	table.sst = OpenSStable(&utils.FileOptions{
		FileName: tableName,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    0,
		FID:      table.fid,
	})
	table.IncrRef()
	if err := table.sst.Init(); err != nil {
		common.Err(err)
	}
	iterator := table.NewTableIterator(&model.Options{IsAsc: true})
	iterator.Rewind()
	for iterator.Valid() {
		entry := iterator.Item().Item
		fmt.Printf("key=%s, value=%s, meta:%d, version=%d \n",
			model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
		iterator.Next()
	}
}
