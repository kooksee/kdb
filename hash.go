package kdb

import (
	"github.com/dgraph-io/badger"
	"github.com/kooksee/kdb/consts"
)

type KHash struct {
	name string

	prefix   []byte
	firstKey []byte
	lastKey  []byte

	count int
	db    *KDB
}

// NewKHash 初始化khash
func NewKHash(name string, db *KDB) *KHash {
	kh := &KHash{name: name, db: db}

	kh.count = db.Len(kh.Prefix())

	if err := db.recordPrefix(kh.Prefix()); err != nil {
		GetLog().Error("recordPrefix error", "err", err.Error())
		panic("")
	}

	return kh
}

// Prefix 前缀
func (k *KHash) Prefix() []byte {
	if len(k.prefix) == 0 {
		k.prefix = []byte(F("%s%s%s%s", consts.KHASH, consts.Separator, k.name, consts.Separator))
	}
	return k.prefix
}

func (k *KHash) FirstKey() []byte {
	if len(k.firstKey) == 0 {
		k.firstKey = k.Prefix()
	}
	return k.firstKey
}

func (k *KHash) LastKey() []byte {
	if len(k.lastKey) == 0 {
		k.lastKey = append(k.Prefix(), consts.MAXBYTE)
	}
	return k.lastKey
}

func (k *KHash) K(key []byte) []byte {
	return append(k.Prefix(), key...)
}

func (k *KHash) Get(key []byte) []byte {
	var val []byte
	var err error

	if err := k.db.GetWithTx(func(txn *badger.Txn) error {
		val, err = k.get(txn, key)
		return err
	}); err != nil {
		GetLog().Error("khash get error", "err", err.Error())
		return nil
	}

	return val
}

func (k *KHash) Del(keys ... []byte) error {
	return k.db.UpdateWithTx(func(txn *badger.Txn) error {
		return k.hDel(txn, keys...)
	})
}

func (k *KHash) Exist(key []byte) bool {
	var b bool
	var err error

	if err := k.db.GetWithTx(func(txn *badger.Txn) error {
		b, err = k.exists(txn, key)
		return err
	}); err != nil {
		GetLog().Error("khash exist error", "err", err.Error())
		return false
	}
	return b
}

func (k *KHash) Drop() error {
	return k.db.UpdateWithTx(func(txn *badger.Txn) error {
		k.count = 0
		return k.db.Drop(txn, k.Prefix())
	})
}

func (k *KHash) Len() int {
	return k.count
}

func (k *KHash) Set(key, value []byte) error {
	return k.db.UpdateWithTx(func(txn *badger.Txn) error {
		return k.set(txn, key, value)
	})
}

func (k *KHash) PopRandom(n int, fn func(key, value []byte) error) error {
	return k.db.UpdateWithTx(func(txn *badger.Txn) error {
		return k.popRandom(txn, n, fn)
	})
}

func (k *KHash) Pop(fn func(key, value []byte) error) error {
	return k.db.UpdateWithTx(func(txn *badger.Txn) error {
		return k.pop(txn, k.Prefix(), fn)
	})
}

func (k *KHash) PopN(n int, fn func(key, value []byte) error) error {
	return k.db.UpdateWithTx(func(txn *badger.Txn) error {
		return k.popN(txn, k.Prefix(), n, fn)
	})
}

// Union 合并
func (k *KHash) Union(otherNames ... string) error {
	return k.db.UpdateWithTx(func(txn *badger.Txn) error {
		for _, otherName := range otherNames {
			if err := k.union(txn, otherName); err != nil {
				return err
			}
		}
		return nil
	})
}

func (k *KHash) BatchView(fn func(*KHBatch) error) error {
	return k.db.GetWithTx(func(txn *badger.Txn) error {
		return fn(&KHBatch{kh: k, txn: txn})
	})
}

func (k *KHash) BatchUpdate(fn func(k *KHBatch) error) error {
	return k.db.UpdateWithTx(func(txn *badger.Txn) error {
		return fn(&KHBatch{kh: k, txn: txn})
	})
}
