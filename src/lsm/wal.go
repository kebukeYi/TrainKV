package lsm

type WalHeader struct {
}

type WAL struct {
	header *WalHeader
}

func NewWAL() *WAL {
	return nil
}

func (w *WAL) init() {

}
