package kdb

import (
	"github.com/dgraph-io/badger"
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
	return k.kh.db.get(k.txn, k.kh.K(key))
}

func (k *KHBatch) MDel(keys ... []byte) (err error) {
	return k.kh.db.mDel(k.txn, keys...)
}

func (k *KHBatch) Exist(key []byte) (bool, error) {
	return k.kh.db.exist(k.txn, key)
}

func (k *KHBatch) PopRandom(n int, fn func(b *KHBatch, i int, key, value []byte) bool) error {
	return k.kh.db.PopRandom(k.txn, k.kh.Prefix(), n, func(i int, key, value []byte) bool {
		return fn(k, i, key, value)
	})
}

func (k *KHBatch) Map(fn func(batch *KHBatch, i int, key, value []byte) bool) error {
	return k.kh.db.PrefixRange(k.txn, k.kh.Prefix(), func(i int, key, value []byte) bool {
		return fn(k, i, key, value)
	})
}

func (k *KHBatch) Filter(filter func(batch *KHBatch, i int, key, value []byte) bool, fn func(batch *KHBatch, i int, key, value []byte) bool) error {
	return k.kh.db.PrefixRange(k.txn, k.kh.Prefix(), func(i int, key, value []byte) bool {
		if filter(k, i, key, value) {
			return fn(k, i, key, value)
		}
		return true
	})
}

func (k *KHBatch) GetSet(key []byte, otherHash string) (val []byte, err error) {
	val, err = k.Get(k.kh.K(key))
	if err != nil {
		return nil, err
	}
	return val, (&KHBatch{txn: k.txn, kh: k.kh.db.KHash(otherHash)}).Set(key, val)
}

func (k *KHBatch) Range(fn func(b *KHBatch, i int, key, value []byte) bool) error {
	return k.kh.db.PrefixRange(k.txn, k.kh.Prefix(), func(i int, key, value []byte) bool {
		return fn(k, i, key, value)
	})
}

func (k *KHBatch) Reverse(fn func(b *KHBatch, i int, key, value []byte) bool) error {
	return k.kh.db.PrefixReverse(k.txn, k.kh.Prefix(), func(i int, key, value []byte) bool {
		return fn(k, i, key, value)
	})
}

func (k *KHBatch) Random(n int, fn func(b *KHBatch, i int, key, value []byte) bool) error {
	return k.kh.db.ScanRandom(k.txn, k.kh.Prefix(), n, func(i int, key, value []byte) bool {
		return fn(k, i, key, value)
	})
}
