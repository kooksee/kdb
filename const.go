package kdb

var (
	prefix     = []byte("px:")
	dataPrefix = []byte("@@:")
	prefixBk   = []byte("pxb:")
)

func withPrefix(name []byte) []byte {
	return append(prefix, name...)
}
