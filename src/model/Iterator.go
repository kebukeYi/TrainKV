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

type Item struct {
	Item *Entry
}

type Options struct {
	Prefix []byte
	IsAsc  bool
}
