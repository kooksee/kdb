package kdb

import "github.com/dgraph-io/badger"

type KBatch struct {
	kh  *KHash
	txn *badger.Txn
}

func (k *KBatch) Set(key, value []byte) error {
	return k.kh.db.mSet(k.txn, &KV{k.kh.K(key), value})
}

func (k *KBatch) MSet(kvs ... *KV) error {
	return k.kh.db.mSet(k.txn, KVMap(kvs, func(_ int, kv *KV) *KV {
		kv.Key = k.kh.K(kv.Key)
		return kv
	})...)
}

func (k *KBatch) Get(key []byte) (v []byte, err error) {
	vals, err := k.kh.db.mGet(k.txn, k.kh.K(key))
	if err != nil {
		return nil, err
	}

	return vals[0], nil
}

func (k *KBatch) MGet(keys ... []byte) (vals [][]byte, err error) {
	return k.kh.db.mGet(k.txn, BMap(keys, func(i int, k1 []byte) []byte {
		return k.kh.K(k1)
	})...)
}

func (k *KBatch) MDel(keys ... []byte) error {
	return k.kh.db.mDel(k.txn, keys...)
}

func (k *KBatch) Exist(key []byte) (bool, error) {
	return k.kh.db.exist(key)
}

func (k *KBatch) Len() (int, error) {
	return k.kh.db.count(k.txn, k.kh.prefix)
}

func (k *KBatch) INCRBY(n int) error {
	return k.kh.db.INCRBY(k.txn, k.kh.countKey, n)
}
