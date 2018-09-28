package kdb

var (
	prefix          = []byte("px:")
	dataPrefix      = []byte("@:")
	khashSizePrefix = []byte("khashSize:")
)

func withPrefix(name []byte) []byte {
	return append(prefix, name...)
}
