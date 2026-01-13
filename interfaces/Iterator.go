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
}

type Options struct {
	Prefix     []byte
	IsAsc      bool // 是否升序遍历, 默认是 true;
	IsSetCache bool // 遍历时,是否保存至缓存中;
}
