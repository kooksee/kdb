package kdb

type IKDB interface {
	KHashExist(name []byte) (bool, error)
	KHash(name []byte) IKHash
	Close() error
	KHashNames() (names chan string, err error)
	ScanAll(fn func(key, value []byte)) error
}

type IKHash interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	MSet(kv ... KV) error
	Del(key ... []byte) error
	Exist(key []byte) (bool, error)
	Drop() error
	Len() (int, error)
	PopRandom(n int, fn func(key, value []byte) error) error
	Pop(fn func(key, value []byte) error) error
	PopN(n int, fn func(key, value []byte) error) error
	Range(fn func(key, value []byte) error) error
	Reverse(fn func(key, value []byte) error) error
	Map(fn func(key, value []byte) ([]byte, error)) error
	Union(otherNames ... []byte) error
	WithBatch(fn func(b IKHBatch) error) error
}

type IKHBatch interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	MDel(keys ... []byte) error
	Exist(key []byte) (bool, error)
	PopRandom(n int, fn func(key, value []byte) error) error
	Pop(fn func(key, value []byte) error) error
	PopN(n int, fn func(key, value []byte) error) error
	Map(fn func(key, value []byte) ([]byte, error)) error
	GetSet(key, value []byte) (val []byte, err error)
	Range(fn func(key, value []byte) error) error
	Reverse(fn func(key, value []byte) error) error
	Random(n int, fn func(key, value []byte) error) error
}
