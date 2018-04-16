package kdb

import "math"

var Separator = []byte{'/'}

// key字节范围
const (
	MINBYTE byte = 0
	MAXBYTE byte = math.MaxUint8
)

// 类型前缀
const (
	METADATA = 'm'
	STRING   = 's'
	HASH     = 'h'
	LIST     = 'l'
	NONE     = '0'
)

// 向前(向后)迭代查询
const (
	IterForward  = iota
	IterBackward
)
