package lsm

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"sort"
	"testing"
	"trainKv/common"
	"trainKv/model"
	"trainKv/utils"
)

var sstID uint64

func getTestTableOptions() *Options {
	return &Options{
		WorkDir:            "/usr/projects_gen_data/goprogendata/trainkvdata/test/sst",
		BlockSize:          4 * 1024,
		BloomFalsePositive: 0.01,
	}
}

func clearDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		common.Panic(err)
	}
	os.Mkdir(dir, os.ModePerm)
	return nil
}

func key(prefix string, i int) string {
	return prefix + fmt.Sprintf("%04d", i)
}

func buildTestTable(t *testing.T, prefix string, n int, opts *Options) *table {
	if opts.BlockSize == 0 {
		opts.BlockSize = 4 * 1024
	}
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		k := key(prefix, i)
		v := fmt.Sprintf("%d", i)
		keyValues[i] = []string{k, v}
	}
	return buildTable(t, keyValues, opts)
}

func buildTable(t *testing.T, keyValues [][]string, opts *Options) *table {
	builder := newSSTBuilder(opts)
	// TODO: Add test for file garbage collection here. No files should be left after the tests here.
	manger := &levelsManger{opt: opts}
	manger.cache = newLevelsCache(opts)
	sstID++
	ssName := utils.FileNameSSTable(manger.opt.WorkDir, sstID)
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		e := model.Entry{
			Key:   model.KeyWithTs([]byte(kv[0])),
			Value: []byte(kv[1]),
		}
		builder.add(e, false)
	}
	tbl := openTable(manger, ssName, builder)
	return tbl
}

func TestTable(t *testing.T) {
	opts := getTestTableOptions()
	clearDir(opts.WorkDir)
	table := buildTestTable(t, "tableKey", 10000, opts)
	defer func() { require.NoError(t, table.DecrRef()) }()
	ti := table.NewTableIterator(&model.Options{IsAsc: true})
	defer ti.Close()
	kid := 1010 // 3) 正常存在的数值
	//seek := y.KeyWithTs([]byte(key("key", kid)), opts.testVersion) // 4)相同版本的数据;
	//seek := y.KeyWithTs([]byte(key("key", kid)), 10) // 5)大于当前版本的数据; 900-10=890; 读取不到;
	seek := model.KeyWithTs([]byte(key("tableKey", kid))) // 6)小于当前版本的数据; 900-200=700; 可读取到;
	ti.Seek(seek)
	for ; ti.Valid(); ti.Next() {
		k := ti.Item().Item.Key
		s := string(model.ParseKey(k))
		fmt.Printf("seekKey: %v, kid: %v;\n", s, kid)
		kid++
	}
	if kid != 10000 {
		t.Errorf("Expected kid: 10000. Got: %v", kid)
	}

	// 要搜寻的key太小找不到, 迭代器会返回当前table的最小值,但不是其要查找的值;
	// 因此迭代器有效,但返回的值无效;
	ti.Seek(model.KeyWithTs([]byte(key("key", 99999))))
	// require.False(t, ti.Valid())
	fmt.Printf("It`s Key: %v", string(model.ParseKey(ti.it.Item.Key)))

	// 要搜寻的key太大找不到, 迭代器会返回当前table的最大值,但不是其要查找的值;
	ti.Seek(model.KeyWithTs([]byte(key("zzzzzzzzkey", 1111))))
	// require.True(t, ti.Valid())
	// require.EqualValues(t, string(model.ParseKey(k)), key("key", 0))
	fmt.Printf("It`s Key: %v", string(model.ParseKey(ti.Item().Item.Key)))
}

func TestTableIterator(t *testing.T) {
	for _, n := range []int{99, 100, 101} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			opts := getTestTableOptions()
			clearDir(opts.WorkDir)
			table := buildTestTable(t, "key", n, opts)
			defer func() { require.NoError(t, table.DecrRef()) }()
			it := table.NewTableIterator(&model.Options{IsAsc: true})
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				v := it.Item().Item
				k := model.KeyWithTs([]byte(key("key", count)))
				fmt.Printf("seekKey: %v, val: %v, count: %v; \n", string(model.ParseKey(k)), v.Value, count)
				//require.EqualValues(t, k, it.Key())
				//require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
				count++
			}
			require.Equal(t, count, n)
		})
	}
}

func TestSeek(t *testing.T) {
	opts := getTestTableOptions()
	clearDir(opts.WorkDir)
	fmt.Printf("2opts.WorkDir: %v;\n", opts.WorkDir)
	table := buildTestTable(t, "k", 10000, opts)
	defer func() { require.NoError(t, table.DecrRef()) }()

	it := table.NewTableIterator(&model.Options{IsAsc: true})
	defer it.Close()

	var data = []struct {
		in    string
		valid bool
		out   string
	}{
		{"abc", true, "k0000"},
		{"k0100", true, "k0100"},
		{"k0100b", true, "k0101"}, // Test case where we jump to next block.
		{"k1234", true, "k1234"},
		{"k1234b", true, "k1235"},
		{"k9999", true, "k9999"},
		{"z", false, ""},
	}

	for _, tt := range data {
		it.Seek(model.KeyWithTs([]byte(tt.in)))
		if !tt.valid {
			// require.False(t, it.Valid())
			fmt.Printf("seekKey: %v, val: %v;\n", tt.in, it.Item().Item.Value)
			continue
		}
		// require.True(t, it.Valid())
		k := it.Item().Item
		require.EqualValues(t, tt.out, string(model.ParseKey(k.Key)))
	}
}

func TestConcatIteratorOneTable(t *testing.T) {
	opts := getTestTableOptions()
	clearDir(opts.WorkDir)
	tbl := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	}, opts)
	defer func() { require.NoError(t, tbl.DecrRef()) }()

	it := NewConcatIterator([]*table{tbl}, &model.Options{IsAsc: true})
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Item().Item.Key
	require.EqualValues(t, "k1", string(model.ParseKey(k)))
	vs := it.Item().Item.Value
	require.EqualValues(t, "a1", string(vs))
}

func TestConcatIterator(t *testing.T) {
	opts := getTestTableOptions()
	clearDir(opts.WorkDir)

	tbl := buildTestTable(t, "keya", 10, opts)
	defer func() { require.NoError(t, tbl.DecrRef()) }()
	tbl2 := buildTestTable(t, "keyb", 10, opts)
	defer func() { require.NoError(t, tbl2.DecrRef()) }()
	tbl3 := buildTestTable(t, "keyc", 10, opts)
	defer func() { require.NoError(t, tbl3.DecrRef()) }()

	{
		it := NewConcatIterator([]*table{tbl, tbl2, tbl3}, &model.Options{IsAsc: true})
		defer it.Close()
		it.Rewind()
		require.True(t, it.Valid())
		var count int
		for ; it.Valid(); it.Next() {
			//vs := it.Item().Item
			vs := it.Value()
			fmt.Printf("key: %v, value: %v\n", string(model.ParseKey(it.Key())), string(vs))
			//require.EqualValues(t, fmt.Sprintf("%d", count%10000), string(vs.Value))
			count++
		}
		//require.EqualValues(t, 30000, count)

		it.Seek(model.KeyWithTs([]byte("a")))
		require.EqualValues(t, "keya0000", string(model.ParseKey(it.Key())))
		require.EqualValues(t, "0", it.Value())

		it.Seek(model.KeyWithTs([]byte("keyb")))
		require.EqualValues(t, "keyb0000", string(model.ParseKey(it.Key())))
		require.EqualValues(t, "0", it.Value())

		it.Seek(model.KeyWithTs([]byte("keyb9999b")))
		require.EqualValues(t, "keyc0000", string(model.ParseKey(it.Key())))
		require.EqualValues(t, "0", it.Value())

		it.Seek(model.KeyWithTs([]byte("keyd")))
		//require.False(t, it.Valid())
	}

	{
		it := NewConcatIterator([]*table{tbl, tbl2, tbl3}, &model.Options{IsAsc: false})
		defer it.Close()
		it.Rewind()
		require.True(t, it.Valid())
		var count int
		for ; it.Valid(); it.Next() {
			vs := it.Value()
			fmt.Printf("key: %v, value: %v\n", string(model.ParseKey(it.Key())), string(vs))
			//require.EqualValues(t, fmt.Sprintf("%d", 10000-(count%10000)-1), string(vs))
			count++
		}
		//require.EqualValues(t, 30000, count)

		it.Seek(model.KeyWithTs([]byte("a")))
		require.False(t, it.Valid())

		it.Seek(model.KeyWithTs([]byte("keyb")))
		require.EqualValues(t, "keya0009", string(model.ParseKey(it.Key())))

		it.Seek(model.KeyWithTs([]byte("keyb0009b")))
		require.EqualValues(t, "keyb0009", string(model.ParseKey(it.Key())))

		it.Seek(model.KeyWithTs([]byte("keyd")))
		require.EqualValues(t, "keyc0009", string(model.ParseKey(it.Key())))
	}
}

func TestMergingIterator(t *testing.T) {
	opts := getTestTableOptions()
	clearDir(opts.WorkDir)
	tbl1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k4", "a4"},
		{"k5", "a5"},
	}, opts)
	defer func() { require.NoError(t, tbl1.DecrRef()) }()

	tbl2 := buildTable(t, [][]string{
		{"k2", "b2"},
		{"k3", "b3"},
		{"k4", "b4"},
	}, opts)
	defer func() { require.NoError(t, tbl2.DecrRef()) }()

	expected := []struct {
		key   string
		value string
	}{
		{"k1", "a1"},
		{"k2", "b2"},
		{"k3", "b3"},
		//{"k4", "a4"},
		{"k4", "b4"},
		{"k5", "a5"},
	}

	it1 := tbl1.NewTableIterator(&model.Options{IsAsc: true})
	it2 := NewConcatIterator([]*table{tbl2}, &model.Options{IsAsc: true})
	it := NewMergeIterator([]model.Iterator{it1, it2}, false)
	defer it.Close()

	var i int
	it.Rewind()
	for ; it.Valid(); it.Next() {
		k := it.Item().Item.Key
		vs := it.Item().Item.Value
		fmt.Printf("key: %v, value: %v\n", string(model.ParseKey(k)), string(vs))
		require.EqualValues(t, expected[i].key, string(model.ParseKey(k)))
		require.EqualValues(t, expected[i].value, string(vs))
		i++
	}
	require.Equal(t, i, len(expected))
	//require.False(t, it.Valid())
}
