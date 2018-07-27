package kdb

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type kHBatch struct {
	IKHBatch

	kh  IKHash
	txn *leveldb.Transaction
}

func (k *kHBatch) Set(key, value []byte) error {
	return k.txn.Put(key, value, nil)
}

func (k *kHBatch) Get(key []byte) ([]byte, error) {
	return k.kh.get(k.txn, key)
}

func (k *kHBatch) MDel(keys ... []byte) error {
	return k.kh.del(k.txn, keys...)
}

func (k *kHBatch) Exist(key []byte) (bool, error) {
	return k.kh.exist(k.txn, key)
}

func (k *kHBatch) PopRandom(n int, fn func(key, value []byte) error) error {
	return k.kh.popRandom(k.txn, n, fn)
}

func (k *kHBatch) Pop(fn func(key, value []byte) error) error {
	return k.kh.pop(k.txn, fn)
}

func (k *kHBatch) PopN(n int, fn func(key, value []byte) error) error {
	return k.kh.popN(k.txn, n, fn)
}

func (k *kHBatch) Map(fn func(key, value []byte) ([]byte, error)) error {
	return k.kh._map(k.txn, fn)
}

func (k *kHBatch) GetSet(key, value []byte) (val []byte, err error) {
	return k.kh.getSet(k.txn, key, value)
}

func (k *kHBatch) Range(fn func(key, value []byte) error) error {
	return k.kh._range(k.txn, fn)
}

func (k *kHBatch) Reverse(fn func(key, value []byte) error) error {
	return k.kh._range(k.txn, fn)
}

func (k *kHBatch) Random(n int, fn func(key, value []byte) error) error {
	return k.kh.scanRandom(k.txn, n, fn)
}
