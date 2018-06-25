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

	err = k.db.set(txn, k1, value)
	if err != nil {
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

func (k *KHash) popRandom(txn *badger.Txn, n int, fn func(key, value []byte) error) error {
	return k.scanRandom(txn, n, func(key, value []byte) error {
		if err := fn(key, value); err != nil {
			return err
		}

		if err := k.hDel(txn, key); err != nil {
			return err
		}

		return nil
	})
}

func (k *KHash) pop(txn *badger.Txn, fn func(key, value []byte) error) error {
	return k.db.ReverseWithPrefix(txn, k.Prefix(), func(key, value []byte) error {

		if err := fn(key, value); err != nil {
			return err
		}

		if err := k.hDel(txn, key); err != nil {
			return err
		}

		return nil
	})
}

func (k *KHash) popN(txn *badger.Txn, n int, fn func(key, value []byte) error) error {
	return k.db.ReverseWithPrefix(txn, k.Prefix(), func(key, value []byte) error {

		if n < 1 {
			return Stop
		}

		if err := fn(key, value); err != nil {
			return err
		}

		if err := k.hDel(txn, key); err != nil {
			return err
		}

		n--

		return nil
	})
}

// ScanRandom 根据前缀随机扫描
func (k *KHash) scanRandom(txn *badger.Txn, count int, fn func(key, value []byte) error) error {

	if k.count < count {
		return k.db.RangeWithPrefix(txn, k.Prefix(), fn)
	}

	m := 0
	var err error
	rmd := GenRandom(0, k.count, count)
	return k.db.RangeWithPrefix(txn, k.Prefix(), func(key, value []byte) error {
		if rmd[m] {
			err = fn(key, value)
		}
		m++
		return err
	})
}
