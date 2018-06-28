package consts

import "math"

var (
	Prefix     = []byte("px:")
	DataPrefix = []byte("@@:")

	// key字节范围
	MINBYTE byte = 0
	MAXBYTE byte = math.MaxUint8

	DbNamePrefix = "db"

	// 类型前缀
	KHASH = []byte("h")

	// 向前(向后)迭代查询
	IterForward  = 0
	IterBackward = 1

	StatueOk      = []byte("ok")
	StatueDeleted = []byte("deleted")
)

func WithPrefix(name []byte) []byte {
	return append(Prefix, name...)
}
