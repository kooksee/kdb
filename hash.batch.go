package kdb

import (
	"github.com/dgraph-io/badger"
)

type KHBatch struct {
	kh  *KHash
	txn *badger.Txn
}

func (k *KHBatch) Set(key, value []byte) error {
	return k.kh.set(k.txn, key, value)
}

func (k *KHBatch) Get(key []byte) ([]byte, error) {
	return k.kh.get(k.txn, key)
}

func (k *KHBatch) MDel(keys ... []byte) error {
	return k.kh.hDel(k.txn, keys...)
}

func (k *KHBatch) Exist(key []byte) (bool, error) {
	return k.kh.exists(k.txn, key)
}

//func (k *KHBatch) PopRandom(n int, fn func(b *KHBatch, key, value []byte) error) error {
//	return k.kh.popRandom(k.txn, n, func(key, value []byte) error {
//		return fn(k, key, value)
//	})
//}

func (k *KHBatch) Pop(fn func(b *KHBatch, key, value []byte) error) error {
	return k.kh.pop(k.txn, func(key, value []byte) error {
		return fn(k, key, value)
	})
}

func (k *KHBatch) PopN(n int, fn func(b *KHBatch, key, value []byte) error) error {
	return k.kh.popN(k.txn, n, func(key, value []byte) error {
		return fn(k, key, value)
	})
}

//func (k *KHBatch) Each(fn func(key, value []byte) error) error {
//	return k.kh.db.RangeWithPrefix(k.txn, k.kh.Prefix(), fn)
//}

func (k *KHBatch) Map(fn func(b *KHBatch, key, value []byte) error) error {
	return k.kh.db.Range(k.txn, k.kh.FirstKey(), k.kh.LastKey(), func(k1, v1 []byte) error {

		GetLog().Info("map KHBatch", "key", string(k1), "value", string(v1))
		return fn(k, k1, v1)
	})
}

//func (k *KHBatch) FilterWithFunc(filter func(key, value []byte) bool, fn func(key, value []byte) error) error {
//	return k.kh.db.RangeWithPrefix(k.txn, k.kh.Prefix(), func(key, value []byte) error {
//		if filter(key, value) {
//			return fn(key, value)
//		}
//		return nil
//	})
//}

//func (k *KHBatch) Filter(filter bool, fn func(key, value []byte) error) error {
//	return k.kh.db.RangeWithPrefix(k.txn, k.kh.Prefix(), func(key, value []byte) error {
//		if filter {
//			return fn(key, value)
//		}
//		return nil
//	})
//}

func (k *KHBatch) GetSet(key []byte, otherHash string) (val []byte, err error) {
	val, err = k.Get(k.kh.K(key))
	if err != nil {
		return nil, err
	}
	return val, (&KHBatch{txn: k.txn, kh: k.kh.db.KHash(otherHash)}).Set(key, val)
}

func (k *KHBatch) Range(fn func(key, value []byte) error) error {
	return k.kh.db.Range(k.txn, k.kh.Prefix(), k.kh.LastKey(), func(key, value []byte) error {
		return fn(key, value)
	})
}

func (k *KHBatch) Reverse(fn func(key, value []byte) error) error {
	return k.kh.db.ReverseWithPrefix(k.txn, k.kh.Prefix(), func(key, value []byte) error {
		return fn(key, value)
	})
}

//func (k *KHBatch) Random(n int, fn func(key, value []byte) error) error {
//	return k.kh.scanRandom(k.txn, n, func(key, value []byte) error {
//		return fn(key, value)
//	})
//}
