package kdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"math/big"
	"bytes"
	"io"
	"sync/atomic"
)

type kDb struct {
	IKDB

	db   *leveldb.DB
	hmap map[string]*kHash

	khashSize int64
}

// 保存khash的数量
func (k *kDb) khashSizeSave(tx *leveldb.Transaction) error {
	atomic.AddInt64(&k.khashSize, 1)
	return errWithMsg(
		"khashSizeSave error",
		tx.Put(khashSizePrefix, big.NewInt(int64(k.khashSize)).Bytes(), nil),
	)
}

// 加载khash数量
func (k *kDb) khashSizeLoad() {
	dt, err := k.get(nil, khashSizePrefix)
	mustNotErr("khashSizeLoad", err)
	k.khashSize = big.NewInt(0).SetBytes(dt).Int64()
}

func (k *kDb) KHashExist(name []byte) bool {
	dt, err := k.db.Has(withPrefix(name), nil)
	if err != nil {
		return false
	}
	return dt
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

func (k *kDb) withTxn(fn func(tx *leveldb.Transaction) error) error {
	txn, err := k.db.OpenTransaction()
	return errWithMsg("kdb withTxn error", err, errCurry(fn, txn), errCurry(txn.Commit))
}

// getPrefix 获得真正的前缀
func (k *kDb) getPrefix(tx *leveldb.Transaction, prefix []byte) ([]byte, error) {
	val, err := k.get(tx, withPrefix(prefix))
	return val, errWithMsg("kdb getPrefix error", err)
}

// recordPrefix 存储并得到前缀
func (k *kDb) recordPrefix(prefix []byte) (px []byte, err error) {
	errMsg := "kdb recordPrefix error"
	return px, k.withTxn(func(tx *leveldb.Transaction) error {
		key := withPrefix(prefix)

		ext, err := k.get(tx, key)
		if err != nil {
			return err
		}

		// 存在就不再存储
		if ext != nil {
			px = ext
			return nil
		}

		px = append(big.NewInt(int64(k.khashSize + 1)).Bytes(), dataPrefix...)
		return errWithMsg(
			errMsg,
			errCurry(k.set, tx, KV{Key: key, Value: px}),
			errCurry(k.khashSizeSave, tx),
		)
	})
}

// KHashNames 获得所有的khash名字
func (k *kDb) KHashNames() (names chan string, err error) {
	errMsg := "kDb KHashNames error"
	names = make(chan string, 1000)
	return names, errWithMsg(errMsg, k.withTxn(func(tx *leveldb.Transaction) error {
		return k.scanWithPrefix(tx, false, prefix, func(key, value []byte) error {
			names <- string(bytes.TrimPrefix(key, prefix))
			return nil
		})
	}))
}

func (k *kDb) write(tx *leveldb.Transaction, b *leveldb.Batch) error {
	if tx != nil {
		return tx.Write(b, nil)
	} else {
		return k.db.Write(b, nil)
	}
}

func (k *kDb) set(tx *leveldb.Transaction, kv ... KV) error {
	b := &leveldb.Batch{}
	for _, i := range kv {
		b.Put(i.Key, i.Value)
	}
	return errWithMsg("kdb.set error", k.write(tx, b))
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
	return errWithMsg("kdb.del error", k.write(tx, b))
}

func (k *kDb) get(tx *leveldb.Transaction, key []byte) ([]byte, error) {
	if tx != nil {
		val, err := tx.Get(key, nil)
		if err == leveldb.ErrNotFound {
			err = nil
		}
		return val, errWithMsg("kdb get error", err)
	}

	val, err := k.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		err = nil
	}
	return val, errWithMsg("kdb tx get error", err)
}

// 扫描全部
func (k *kDb) ScanAll(fn func(key, value []byte)) error {
	iter := k.db.NewIterator(nil, nil)
	for iter.Next() {
		fn(iter.Key(), iter.Value())
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
