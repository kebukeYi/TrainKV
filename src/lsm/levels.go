package lsm

type levelManger struct {
	levels []*levelHandler
}

type levelHandler struct {
	ssTables []*SSTable
}
