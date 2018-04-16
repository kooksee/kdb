package kdb

import (
	"github.com/dgraph-io/badger"
	"bytes"
)

type KHBatch struct {
	kh  *KHash
	txn *badger.Txn
}

func (k *KHBatch) Set(key, value []byte) error {
	return k.MSet(&KV{k.kh.K(key), value})
}

func (k *KHBatch) MSet(kvs ... *KV) error {
	return k.kh.db.mSet(k.txn, KVMap(kvs, func(_ int, kv *KV) *KV {
		kv.Key = k.kh.K(kv.Key)
		return kv
	})...)
}

func (k *KHBatch) Get(key []byte) ([]byte, error) {
	vals, err := k.kh.db.mGet(k.txn, k.kh.K(key))
	return vals[0], err
}

func (k *KHBatch) MGet(keys ... []byte) (vals [][]byte, err error) {
	return k.kh.db.mGet(k.txn, BMap(keys, func(i int, k1 []byte) []byte {
		return k.kh.K(k1)
	})...)
}

func (k *KHBatch) MDel(keys ... []byte) (err error) {
	return k.kh.db.mDel(k.txn, keys...)
}

func (k *KHBatch) Exist(key []byte) (bool, error) {
	return k.kh.db.exist(k.txn, key)
}

func (k *KHBatch) Len() (int, error) {
	return k.kh.db.Len(k.txn, k.kh.prefix)
}

func (k *KHBatch) PopNIter(n int, fn func(i int, key, value []byte)) error {
	return k.kh.db.PopN(k.txn, true, k.kh.firstKey, k.kh.lastKey, n, func(i int, key, value []byte) bool {
		fn(i, bytes.TrimPrefix(key, k.kh.prefix), value)
		return true
	})
}

func (k *KHBatch) PopN(n int) (vals map[string][]byte, err error) {
	return vals, k.kh.db.PopN(k.txn, true, k.kh.firstKey, k.kh.lastKey, n, func(i int, key, value []byte) bool {
		vals[string(bytes.TrimPrefix(key, k.kh.prefix))] = value
		return true
	})
}

func (k *KHBatch) PopRandomIter(n int, fn func(i int, key, value []byte)) error {
	return k.kh.db.PopRandom(k.txn, k.kh.prefix, n, func(i int, key, value []byte) {
		fn(i, bytes.TrimPrefix(key, k.kh.prefix), value)
	})
}

func (k *KHBatch) Map(fn func(batch *KHBatch, i int, key, value []byte)) error {
	return k.kh.db.Scan(k.txn, k.kh.prefix, 0, func(i int, key, value []byte) bool {
		fn(k, i, bytes.TrimPrefix(key, k.kh.prefix), value)
		return true
	})
}

func (k *KHBatch) GetSet(key []byte, other string) (val []byte, err error) {
	val, err = k.Get(k.kh.K(key))
	return val, (&KHBatch{txn: k.txn, kh: k.kh.db.KHash(other)}).Set(key, val)
}

func (k *KHBatch) Range(fn func(i int, key, value []byte)) error {
	return k.kh.db.ScanRange(k.txn, false, k.kh.firstKey, k.kh.lastKey, func(i int, key, value []byte) bool {
		fn(i, bytes.TrimPrefix(key, k.kh.prefix), value)
		return true
	})
}

func (k *KHBatch) Reverse(fn func(i int, key, value []byte)) error {
	return k.kh.db.ScanRange(k.txn, true, k.kh.firstKey, k.kh.lastKey, func(i int, key, value []byte) bool {
		fn(i, bytes.TrimPrefix(key, k.kh.prefix), value)
		return true
	})
}

func (k *KHBatch) Random(n int, fn func(i int, key, value []byte)) error {
	return k.kh.db.ScanRandom(k.txn, k.kh.prefix, n, func(i int, key, value []byte) bool {
		fn(i, bytes.TrimPrefix(key, k.kh.prefix), value)
		return true
	})
}

func (k *KHBatch) RandomIter(n int) (vals map[string][]byte, err error) {
	return vals, k.kh.db.ScanRandom(k.txn, k.kh.prefix, n, func(i int, key, value []byte) bool {
		vals[string(bytes.TrimPrefix(key, k.kh.prefix))] = value
		return true
	})
}
