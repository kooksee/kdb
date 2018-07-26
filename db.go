package kdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"math/big"
	"bytes"
	"io"
)

type kDb struct {
	IKDB

	db   *leveldb.DB
	hmap map[string]IKHash
}

func (k *kDb) KHashExist(name []byte) (bool, error) {
	return k.db.Has(withPrefix(name), nil)
}

func (k *kDb) KHash(name []byte) IKHash {
	key := string(name)
	if _, ok := k.hmap[key]; !ok {
		k.hmap[key] = newkHash(name, k)
	}
	return k.hmap[key]
}

func (k *kDb) Close() error {
	return k.db.Close()
}

func (k *kDb) WithTxn(fn func(tx *leveldb.Transaction) error) error {
	txn, err := k.db.OpenTransaction()
	if err != nil {
		return errWithMsg("WithTxn OpenTransaction Error", err)
	}

	if err := fn(txn); err != nil {
		return errWithMsg("WithTxn fn Error", err)
	}

	return errWithMsg("WithTxn Error", txn.Commit())
}

func (k *kDb) getPrefix(tx *leveldb.Transaction, prefix []byte) ([]byte, error) {
	val, err := k.get(tx, withPrefix(prefix))
	return val, errWithMsg("kdb getPrefix error", err)
}

// 存储并得到前缀
func (k *kDb) recordPrefix(prefix []byte) (px []byte, err error) {
	errMsg := "kdb recordPrefix error"
	return px, errWithMsg(errMsg, k.WithTxn(func(tx *leveldb.Transaction) error {
		key := withPrefix(prefix)
		ext, err := tx.Get(key, nil)
		if err != nil {
			return err
		}
		// 存在就不再存储
		if len(ext) != 0 {
			px = ext
			return nil
		}

		l, err := k.sizeof([]byte(prefix))
		if err != err {
			return err
		}

		px = append(big.NewInt(int64(l)).Bytes(), dataPrefix...)
		k.scanWithPrefix(tx, false, prefixBk, func(key, value []byte) error {
			px = value
			if err := k.del(tx, append(prefixBk, key...), value); err != nil {
				return err
			} else {
				return io.EOF
			}
		})
		return tx.Put(key, px, nil)
	}))
}

func (k *kDb) saveBk(tx *leveldb.Transaction, key, value []byte) error {
	return k.set(tx, KV{Key: append(prefixBk, key...), Value: value})
}

func (k *kDb) KHashNames() (names []string, err error) {
	errMsg := "kDb KHashNames error"
	return names, errWithMsg(errMsg, k.WithTxn(func(tx *leveldb.Transaction) error {
		return k.scanWithPrefix(tx, false, prefix, func(key, value []byte) error {
			names = append(names, string(bytes.TrimPrefix(key, prefix)))
			return nil
		})
	}))
}

func (k *kDb) set(tx *leveldb.Transaction, kv ... KV) error {
	b := &leveldb.Batch{}
	for _, i := range kv {
		b.Put(i.Key, i.Value)
	}

	if tx != nil {
		return errWithMsg("kdb tx Batch set error", tx.Write(b, nil))
	} else {
		return errWithMsg("kdb set error", k.db.Write(b, nil))
	}
}

func (k *kDb) exist(tx *leveldb.Transaction, name []byte) (bool, error) {
	if tx != nil {
		b, err := tx.Has(name, nil)
		return b, errWithMsg("kdb tx exist error", err)
	}
	b, err := k.db.Has(name, nil)
	return b, errWithMsg("kdb exist error", err)
}

func (k *kDb) del(tx *leveldb.Transaction, keys ... []byte) error {
	b := &leveldb.Batch{}
	for _, k := range keys {
		b.Delete(k)
	}

	if tx != nil {
		return errWithMsg("kdb tx del error", tx.Write(b, nil))
	} else {
		return errWithMsg("kdb del error", k.db.Write(b, nil))
	}

}

func (k *kDb) get(tx *leveldb.Transaction, key []byte) ([]byte, error) {
	if tx != nil {
		val, err := tx.Get(key, nil)
		return val, errWithMsg("kdb get error", err)
	}
	val, err := k.db.Get(key, nil)
	return val, errWithMsg("kdb tx get error", err)
}

// 扫描全部
func (k *kDb) ScanAll(fn func(key, value []byte) error) error {
	iter := k.db.NewIterator(nil, nil)
	for iter.First(); iter.Next(); {
		if err := fn(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	iter.Release()
	return iter.Error()
}

// 范围扫描
func (k *kDb) scanWithPrefix(txn *leveldb.Transaction, isReverse bool, prefix []byte, fn func(key, value []byte) error) error {
	errMsg := "kdb scanWithPrefix error"

	iter := k.db.NewIterator(util.BytesPrefix(prefix), nil)
	if txn != nil {
		iter = txn.NewIterator(util.BytesPrefix(prefix), nil)
	}

	if isReverse && iter.Last() {
		for iter.Prev() {
			err := fn(bytes.TrimPrefix(iter.Key(), prefix), iter.Value())
			if err == io.EOF {
				break
			}

			if err != nil {
				return errWithMsg(errMsg, err)
			}
		}
	}

	if !isReverse && iter.First() {
		for iter.Next() {
			err := fn(bytes.TrimPrefix(iter.Key(), prefix), iter.Value())
			if err == io.EOF {
				break
			}

			if err != nil {
				return errWithMsg(errMsg, err)
			}
		}
	}

	iter.Release()
	return iter.Error()
}

// 根据前缀扫描数据数量
func (k *kDb) sizeof(prefix []byte) (m int, err error) {
	errMsg := "kDb sizeof error"
	size, err := k.db.SizeOf([]util.Range{*util.BytesPrefix(prefix)})
	return int(size.Sum()), errWithMsg(errMsg, err)
}
