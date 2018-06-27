package kdb

import (
	"github.com/dgraph-io/badger"
)

func (k *KHash) get(txn *badger.Txn, key []byte) (val []byte, err error) {
	return k.db.get(txn, k.K(key))
}

func (k *KHash) hDel(txn *badger.Txn, keys ... []byte) error {
	for _, key := range keys {
		if err := k.db.mDel(txn, k.K(key)); err != nil {
			return err
		}
	}

	k.count -= len(keys)
	return nil
}

func (k *KHash) exists(txn *badger.Txn, key []byte) (b bool, err error) {
	return k.db.exist(txn, k.K(key))
}

func (k *KHash) set(txn *badger.Txn, key, value []byte) error {
	k1 := k.K(key)

	b, err := k.db.exist(txn, k1)
	if err != nil {
		GetLog().Error("exist error", "err", err.Error())
		return err
	}

	if err = k.db.set(txn, k1, value); err != nil {
		GetLog().Error("db set error", "err", err.Error())
		return err
	}

	if !b {
		k.count++
	}

	return nil
}

func (k *KHash) union(txn *badger.Txn, otherName string) error {
	h1 := k.db.KHash(otherName)
	return h1.Pop(func(key, value []byte) error {
		return k.set(txn, key, value)
	})
}

//func (k *KHash) popRandom(txn *badger.Txn, n int, fn func(key, value []byte) error) error {
//	return k.scanRandom(txn, n, func(k1, v1 []byte) error {
//		if err := fn(k1, v1); err != nil {
//			return err
//		}
//
//		if err := k.hDel(txn, k1); err != nil {
//			return err
//		}
//
//		return nil
//	})
//}

func (k *KHash) pop(txn *badger.Txn, fn func(key, value []byte) error) error {
	return k.db.ReverseWithPrefix(txn, k.Prefix(), func(k1, v1 []byte) error {

		if err := fn(k1, v1); err != nil {
			return err
		}

		if err := k.hDel(txn, k1); err != nil {
			return err
		}

		return nil
	})
}

func (k *KHash) popN(txn *badger.Txn, n int, fn func(key, value []byte) error) error {
	return k.db.ReverseWithPrefix(txn, k.Prefix(), func(k1, v1 []byte) error {

		if n < 1 {
			return Stop
		}

		if err := fn(k1, v1); err != nil {
			return err
		}

		if err := k.hDel(txn, k1); err != nil {
			return err
		}

		n--

		return nil
	})
}

// ScanRandom 根据前缀随机扫描
//func (k *KHash) scanRandom(txn *badger.Txn, count int, fn func(key, value []byte) error) error {
//
//	if k.count < count {
//		return k.db.RangeWithPrefix(txn, k.Prefix(), fn)
//	}
//
//	m := 0
//	var err error
//	rmd := GenRandom(0, k.count, count)
//	return k.db.RangeWithPrefix(txn, k.Prefix(), func(key, value []byte) error {
//		if rmd[m] {
//			err = fn(key, value)
//		}
//		m++
//		return err
//	})
//}

//func (k *KDB) Len(prefix []byte) (m int) {
//
//	if err := k.db.View(func(txn *badger.Txn) error {
//		return k.RangeWithPrefix(txn, prefix, func(_, _ []byte) error {
//			m++
//			return nil
//		})
//	}); err != nil {
//		GetLog().Error("kdb len error", "err", err.Error())
//	}
//
//	return
//}