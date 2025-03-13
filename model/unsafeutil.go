package model

import _ "unsafe"

// FastRand is a fast thread local random function.
//
//go:linkname FastRand runtime.fastrand
func FastRand() uint32
