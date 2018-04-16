package kdb

import (
	"github.com/dgraph-io/badger"
	"github.com/kataras/iris/core/errors"
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

func (k *KHBatch) PopN(n int, fn func(i int, key, value []byte)) error {
	return k.kh.db.PopN(k.txn, true, k.kh.firstKey, k.kh.lastKey, n, func(i int, key, value []byte) bool {
		fn(i, key, value)
		return true
	})
}

func (k *KHBatch) Map(fn func(batch *KHBatch, i int, key, value []byte)) error {
	return k.kh.db.Scan(k.txn, k.kh.prefix, 0, func(i int, key, value []byte) bool {
		fn(k, i, key, value)
		return true
	})
}

func (k *KHBatch) GetSet(key []byte, other []byte) (val []byte, err error) {
	return val, k.kh.db.UpdateWithTx(func(txn *badger.Txn) error {
		val, err = k.kh.db.get(txn, k.kh.K(key))
		if err != nil {
			return err
		}
		if val == nil {
			return errors.New("key不存在")
		}
		return k.kh.db.set(txn, k.kh.db.K(k.kh.db.KHPrefix(other), key), val)
	})
}
