package consts

import (
	"math"
)

const (
	Separator = ':'

	// key字节范围
	MINBYTE byte = 0
	MAXBYTE byte = math.MaxUint8

	DbNamePrefix = "db"

	// 类型前缀
	KHASH = 'h'
	KLIST = 'l'

	// 向前(向后)迭代查询
	IterForward  = 0
	IterBackward = 1
)

// 记录数据名称,同类型名称不能重复
// db:h:name
// db:l:name
