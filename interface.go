package kdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/json-iterator/go"
)

type IKDB interface {
	KHashExist(name []byte) (bool, error)
	KHash(name []byte) IKHash
	Close() error
	WithTxn(fn func(tx *leveldb.Transaction) error) error
	KHashNames() (names []string, err error)
	ScanAll(fn func(key, value []byte) error) error

	sizeof(prefix []byte) (m int, err error)
	saveBk(tx *leveldb.Transaction, key, value []byte) error
	recordPrefix(prefix []byte) (px []byte, err error)
	getPrefix(tx *leveldb.Transaction, prefix []byte) ([]byte, error)
	set(tx *leveldb.Transaction, kv ... KV) error
	exist(tx *leveldb.Transaction, name []byte) (bool, error)
	del(tx *leveldb.Transaction, keys ... []byte) error
	get(tx *leveldb.Transaction, key []byte) ([]byte, error)
	scanWithPrefix(txn *leveldb.Transaction, isReverse bool, prefix []byte, fn func(key, value []byte) error) error
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
	WithTx(fn func(kh IKHBatch) error) error

	getPrefix() []byte
	k(key []byte) []byte
	set(txn *leveldb.Transaction, kv ... KV) error
	del(txn *leveldb.Transaction, key ... []byte) error
	get(txn *leveldb.Transaction, key []byte) ([]byte, error)
	getJson(txn *leveldb.Transaction, key []byte, path ...interface{}) (jsoniter.Any, error)
	exist(txn *leveldb.Transaction, key []byte) (bool, error)
	_map(txn *leveldb.Transaction, fn func(key, value []byte) ([]byte, error)) error
	union(txn *leveldb.Transaction, others ... []byte) error
	getSet(txn *leveldb.Transaction, key, value []byte) (val []byte, err error)
	popRandom(txn *leveldb.Transaction, n int, fn func(key, value []byte) error) error
	pop(txn *leveldb.Transaction, fn func(key, value []byte) error) error
	popN(txn *leveldb.Transaction, n int, fn func(key, value []byte) error) error
	scanRandom(txn *leveldb.Transaction, count int, fn func(key, value []byte) error) error
	len() (int, error)
	_range(txn *leveldb.Transaction,fn func(key, value []byte) error) error
	_reverse(txn *leveldb.Transaction,fn func(key, value []byte) error) error
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
