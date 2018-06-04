package kdb

import (
	"github.com/dgraph-io/badger"
)

type KHBatch struct {
	kh  *KHash
	txn *badger.Txn
}

func (k *KHBatch) Set(key, value []byte) error {
	return k.MSet(NewKV(k.kh.K(key), value))
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

func (k *KHBatch) PopRandom(n int, fn func(b *KHBatch, key, value []byte) error) error {
	return k.kh.db.PopRandom(k.txn, k.kh.Prefix(), n, func(key, value []byte) error {
		return fn(k, key, value)
	})
}

func (k *KHBatch) Each(fn func(key, value []byte) error) error {
	return k.kh.db.RangeWithPrefix(k.txn, k.kh.Prefix(), fn)
}

func (k *KHBatch) Map(fn func(batch *KHBatch, key, value []byte) error) error {
	return k.kh.db.RangeWithPrefix(k.txn, k.kh.Prefix(), func(key, value []byte) error {
		return fn(k, key, value)
	})
}

func (k *KHBatch) FilterWithFunc(filter func(key, value []byte) bool, fn func(key, value []byte) error) error {
	return k.kh.db.RangeWithPrefix(k.txn, k.kh.Prefix(), func(key, value []byte) error {
		if filter(key, value) {
			return fn(key, value)
		}
		return nil
	})
}

func (k *KHBatch) Filter(filter bool, fn func(key, value []byte) error) error {
	return k.kh.db.RangeWithPrefix(k.txn, k.kh.Prefix(), func(key, value []byte) error {
		if filter {
			return fn(key, value)
		}
		return nil
	})
}

func (k *KHBatch) GetSet(key []byte, otherHash string) (val []byte, err error) {
	val, err = k.Get(k.kh.K(key))
	if err != nil {
		return nil, err
	}

	kh, err := k.kh.db.KHash(otherHash)
	if err != nil {
		return nil, err
	}
	return val, (&KHBatch{txn: k.txn, kh: kh}).Set(key, val)
}

func (k *KHBatch) Range(fn func(key, value []byte) error) error {
	return k.kh.db.RangeWithPrefix(k.txn, k.kh.Prefix(), func(key, value []byte) error {
		return fn(key, value)
	})
}

func (k *KHBatch) Reverse(fn func(key, value []byte) error) error {
	return k.kh.db.ReverseWithPrefix(k.txn, k.kh.Prefix(), func(key, value []byte) error {
		return fn(key, value)
	})
}

func (k *KHBatch) Random(n int, fn func(key, value []byte) error) error {
	return k.kh.db.ScanRandom(k.txn, k.kh.Prefix(), n, func(key, value []byte) error {
		return fn(key, value)
	})
}
