package kdb

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type KHBatch struct {
	kh  *KHash
	txn *leveldb.Transaction
}

func (k *KHBatch) Set(key, value []byte) error {
	return k.txn.Put(key, value, nil)
}

func (k *KHBatch) Get(key []byte) ([]byte, error) {
	return k.kh.get(k.txn, key)
}

func (k *KHBatch) MDel(keys ... []byte) error {
	return k.kh.del(k.txn, keys...)
}

func (k *KHBatch) Exist(key []byte) (bool, error) {
	return k.kh.exist(k.txn, key)
}

func (k *KHBatch) PopRandom(n int, fn func(key, value []byte) error) error {
	return k.kh.popRandom(k.txn, n, fn)
}

func (k *KHBatch) Pop(fn func(key, value []byte) error) error {
	return k.kh.pop(k.txn, fn)
}

func (k *KHBatch) PopN(n int, fn func(key, value []byte) error) error {
	return k.kh.popN(k.txn, n, fn)
}

func (k *KHBatch) Map(fn func(key, value []byte) ([]byte, error)) error {
	return k.kh._map(k.txn, fn)
}

func (k *KHBatch) GetSet(key, value []byte) (val []byte, err error) {
	return k.kh.getSet(k.txn, key, value)
}

func (k *KHBatch) Range(fn func(key, value []byte) error) error {
	return k.kh.db.scanWithPrefix(k.txn, false, k.kh.Prefix(), fn)
}

func (k *KHBatch) Reverse(fn func(key, value []byte) error) error {
	return k.kh.db.scanWithPrefix(k.txn, true, k.kh.Prefix(), fn)
}

func (k *KHBatch) Random(n int, fn func(key, value []byte) error) error {
	return k.kh.scanRandom(k.txn, n, fn)
}
