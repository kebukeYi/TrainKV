package interfaces

import "github.com/kebukeYi/TrainKV/v2/model"

type Iterator interface {
	Name() string
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Seek(key []byte)
	Close() error
}

type Item struct {
	Item model.Entry

	// 大 value 惰性读: VP.Len > 0 表示 value 存于 vlog, Value() 按需解码;
	// 小 value 的 Item 这两个字段为零值, 行为与旧版一致;
	VP   model.ValuePtr
	Vlog model.ValueReader
}

// Value 返回条目值;
// 小 value 直接返回 Item.Value; 大 value 从 vlog 按需读取并拷贝 (须在 Next() 前消费);
func (it Item) Value() ([]byte, error) {
	if it.VP.Len == 0 || it.Vlog == nil {
		return it.Item.Value, nil
	}

	read, callBack, err := it.Vlog.Read(&it.VP)
	if err != nil {
		if callBack != nil {
			callBack()
		}
		return nil, err
	}
	val := model.SafeCopy(nil, read)
	callBack()
	return val, nil
}

type Options struct {
	Prefix     []byte
	IsAsc      bool // 是否升序遍历, 默认是 true;
	IsSetCache bool // 遍历时,是否保存至缓存中;
}
