package kdb

import "github.com/dgraph-io/badger"

type KHBatch struct {
	kh  *KHash
	txn *badger.Txn
}

func (k *KHBatch) Set(key, value []byte) error {
	return k.MSet(&KV{k.kh.K(key), value})
}

func (k *KHBatch) MSet(kvs ... *KV) (err error) {
	err = k.kh.db.mSet(k.txn, KVMap(kvs, func(_ int, kv *KV) *KV {
		kv.Key = k.kh.K(kv.Key)
		return kv
	})...)
	if err != nil {
		return err
	}

	return nil
}

func (k *KHBatch) Get(key []byte) (v []byte, err error) {
	vals, err := k.kh.db.mGet(k.txn, k.kh.K(key))
	if err != nil {
		return nil, err
	}

	return vals[0], nil
}

func (k *KHBatch) MGet(keys ... []byte) (vals [][]byte, err error) {
	return k.kh.db.mGet(k.txn, BMap(keys, func(i int, k1 []byte) []byte {
		return k.kh.K(k1)
	})...)
}

func (k *KHBatch) MDel(keys ... []byte) (err error) {
	err = k.kh.db.mDel(k.txn, keys...)
	if err != nil {
		return err
	}

	return nil
}

func (k *KHBatch) Exist(key []byte) (bool, error) {
	return k.kh.db.exist(k.txn, key)
}

func (k *KHBatch) Len() (int, error) {
	return k.kh.db.Len(k.txn, k.kh.prefix)
}

func (k *KHBatch) PopN(n int) (int, error) {
	k.kh.db.PopN(k.txn, true, k.kh.firstKey, k.kh.lastKey, n, func(i int, key, value []byte) bool {

	})
	return k.kh.db.Len(k.txn, k.kh.prefix)
}
