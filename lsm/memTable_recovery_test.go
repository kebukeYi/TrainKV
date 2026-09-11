package lsm

import (
	"os"
	"strconv"
	"testing"

	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/model"
	"github.com/kebukeYi/TrainKV/v2/skl"
	"github.com/kebukeYi/TrainKV/v2/utils"
)

// 构造一个带空 WAL 的 MemoryTable, 用于 recovery2SkipList 重放测试;
func newRecoveryMemTable(t *testing.T) (*MemoryTable, *WAL) {
	t.Helper()
	dir := t.TempDir()
	opt := GetDefaultOpt(dir)
	walFid := uint64(1)
	fileOpt := &utils.FileOptions{
		Dir:      dir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int32(opt.MemTableSize),
		FID:      walFid,
		FileName: mtFilePath(dir, walFid),
	}
	w := OpenWalFile(fileOpt)
	m := &MemoryTable{
		skipList: skl.NewSkipList(opt.MemTableSize),
		wal:      w,
		name:     strconv.FormatUint(walFid, 10) + MemTableName,
	}
	m.skipList.OnClose = func() {
		_ = m.close(true)
	}
	return m, w
}

func walDataEntry(key string, ts uint64) *model.Entry {
	return &model.Entry{
		Key:   model.KeyWithTs([]byte(key), ts),
		Value: []byte("v-" + key),
		Meta:  common.BitTxn,
	}
}

func walFinEntry(ts uint64) *model.Entry {
	return &model.Entry{
		Key:   []byte(common.TxnKey),
		Value: []byte(strconv.FormatUint(ts, 10)),
		Meta:  common.BitFinTxn,
	}
}

func writeWalEntries(w *WAL, entries ...*model.Entry) {
	for _, e := range entries {
		if err := w.Write(e); err != nil {
			panic(err)
		}
	}
}

// TestRecoveryOrphanFin 孤儿 FIN (无配对 BitTxn) 的重放: 旧版本按条目轮转会把同一事务的
// finTxn 拆进新 WAL (数据随旧表 flush 成 SST), 空事务提交也会留下纯 fin 记录;
// 重放必须跳过孤儿 FIN 而不是 ErrBadTxn, 否则存量库无法重开;
func TestRecoveryOrphanFin(t *testing.T) {
	cases := []struct {
		name     string
		entries  []*model.Entry
		wantKeys map[string]uint64 // 期望恢复出的 key -> ts
		wantTail bool               // 期望 validEndOffset 落在文件末尾(尾部事务完整)
	}{
		{
			name:    "head-orphan-fin",
			entries: []*model.Entry{walFinEntry(2), walDataEntry("k3", 3), walFinEntry(3)},
			wantKeys: map[string]uint64{
				"k3": 3,
			},
			wantTail: true,
		},
		{
			name: "mid-orphan-fin",
			entries: []*model.Entry{
				walDataEntry("k1", 1), walFinEntry(1),
				walFinEntry(2), // 孤儿 FIN
				walDataEntry("k3", 3), walFinEntry(3),
			},
			wantKeys: map[string]uint64{
				"k1": 1,
				"k3": 3,
			},
			wantTail: true,
		},
		{
			name:     "only-orphan-fin",
			entries:  []*model.Entry{walFinEntry(9)},
			wantKeys: map[string]uint64{},
			wantTail: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, w := newRecoveryMemTable(t)
			defer m.close(true)
			writeWalEntries(w, tc.entries...)

			endOff, err := m.recovery2SkipList()
			if err != nil {
				t.Fatalf("recovery2SkipList unexpected err: %v", err)
			}
			for key, ts := range tc.wantKeys {
				val := m.skipList.Get(model.KeyWithTs([]byte(key), ts))
				if val.Version == 0 && val.Value == nil {
					t.Errorf("key %s@%d not recovered", key, ts)
				}
			}
			if tc.wantTail && endOff != w.Size() {
				t.Errorf("validEndOffset=%d, want wal size=%d", endOff, w.Size())
			}
			if !tc.wantTail && endOff != 0 {
				t.Errorf("validEndOffset=%d, want 0 (nothing recoverable)", endOff)
			}
		})
	}
}
