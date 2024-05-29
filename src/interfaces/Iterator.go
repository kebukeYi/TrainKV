package interfaces

import "trainKv/model"

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Seek(key []byte)
	Close() error
}

type Item struct {
	Item *model.Entry
}

type Options struct {
	Prefix []byte
	IsAsc  bool
}
