package kdb

import "math"

var (
	Prefix     = []byte("px:")
	DataPrefix = []byte("@@:")
	PrefixBk   = []byte("pxb:")

	// key字节范围
	MINBYTE byte = 0
	MAXBYTE byte = math.MaxUint8

	StatueOk      = []byte("ok")
	StatueDeleted = []byte("deleted")
)

func WithPrefix(name []byte) []byte {
	return append(Prefix, name...)
}
