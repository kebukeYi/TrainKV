package model

type Iterator interface {
	Name() string
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Seek(key []byte)
	Close() error
}

// ValueReader vlog 读取接口, 供 Item 惰性解码大 value (由 *ValueLog 实现);
type ValueReader interface {
	Read(vp *ValuePtr) ([]byte, func(), error)
}

type Item struct {
	Item Entry
}

type Options struct {
	Prefix []byte
	IsAsc  bool // 是否升序遍历, 默认是 true;
}
