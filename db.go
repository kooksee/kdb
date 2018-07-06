package kdb

import (
	"os"
	"path/filepath"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"math/big"
	"bytes"
)

var Stop = errors.New("STOP")

type KDB struct {
	db   *leveldb.DB
	hmap map[string]*KHash
}

// InitKdb 初始化数据库
func InitKdb(paths ... string) {
	once.Do(func() {
		path := filepath.Join("kdata", "db")
		if len(paths) > 0 {
			path = paths[0]
		}

		if _, err := os.Stat(path); os.IsNotExist(err) {
			if err := os.MkdirAll(path, 0755); err != nil {
				panic(F("could not create directory %s. %s", path, err.Error()))
			}
		}

		db, err := leveldb.OpenFile(path, nil)
		if err != nil {
			panic(Errs("the db start fail", err.Error()))
		}

		kdb = &KDB{db: db, hmap: make(map[string]*KHash)}
	})
}

// GetKdb 得到kdb实例
func GetKdb() *KDB {
	if kdb == nil {
		panic("please init kdb")
	}
	return kdb
}

func (k *KDB) KHashExist(name []byte) (bool, error) {
	return k.db.Has(WithPrefix(name), nil)
}

// NewLDBDatabase returns a LevelDB wrapped object.
func NewLDBDatabase(file string, cache int, handles int) (*leveldb.DB, error) {

	// Ensure we have some minimal caching and file guarantees
	if cache < 16 {
		cache = 16
	}
	if handles < 16 {
		handles = 16
	}

	// Open the db and recover any potential corruptions
	db, err := leveldb.OpenFile(file, &opt.Options{
		OpenFilesCacheCapacity: handles,
		BlockCacheCapacity:     cache / 2 * opt.MiB,
		WriteBuffer:            cache / 4 * opt.MiB, // Two of these are used internally
		Filter:                 filter.NewBloomFilter(10),
	})
	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(file, nil)
	}
	// (Re)check for errors and abort if opening of the db failed
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (k *KDB) KHash(name []byte) *KHash {
	key := string(name)
	if _, ok := k.hmap[key]; !ok {
		k.hmap[key] = NewKHash(name, k)
	}
	return k.hmap[key]
}

func (k *KDB) Close() error {
	return k.db.Close()
}

func (k *KDB) WithTxn(fn func(tx *leveldb.Transaction) error) error {
	txn, err := k.db.OpenTransaction()
	if err != nil {
		return err
	}

	if err := fn(txn); err != nil {
		return err
	}

	return ErrPipeWithMsg("WithTxn Error", txn.Commit())
}

func (k *KDB) getPrefix(tx *leveldb.Transaction, prefix []byte) ([]byte, error) {
	errMsg := "kdb getPrefix error"

	if tx == nil {
		val, err := k.db.Get(WithPrefix(prefix), nil)
		return val, ErrPipeWithMsg(errMsg, err)
	}

	val, err := tx.Get(WithPrefix(prefix), nil)
	return val, ErrPipeWithMsg(errMsg, err)
}

// 存储并得到前缀
func (k *KDB) recordPrefix(prefix []byte) (px []byte, err error) {
	errMsg := "kdb recordPrefix error"
	return px, ErrPipeWithMsg(errMsg, k.WithTxn(func(tx *leveldb.Transaction) error {
		key := WithPrefix(prefix)
		ext, err := tx.Has(key, nil)
		if err != nil {
			return err
		}
		// 存在就不再存储
		if ext {
			return nil
		}

		l, err := k.sizeof([]byte(Prefix))
		if err != err {
			return err
		}

		px = append(big.NewInt(int64(l)).Bytes(), DataPrefix...)
		k.scanWithPrefix(tx, false, PrefixBk, func(key, value []byte) error {
			px = value
			if err := k.del(tx, append(PrefixBk, key...), value); err != nil {
				return err
			} else {
				return Stop
			}
		})
		return tx.Put(key, px, nil)
	}))
}

func (k *KDB) KHashNames() (names []string, err error) {
	errMsg := "kdb KHashNames error"
	return names, ErrPipeWithMsg(errMsg, k.WithTxn(func(tx *leveldb.Transaction) error {
		return k.scanWithPrefix(tx, false, Prefix, func(key, value []byte) error {
			names = append(names, string(bytes.TrimPrefix(key, Prefix)))
			return nil
		})
	}))
}

func (k *KDB) set(tx *leveldb.Transaction, kv ... KV) error {
	b := &leveldb.Batch{}
	for _, i := range kv {
		b.Put(i.Key, i.Value)
	}

	if tx != nil {
		return ErrPipeWithMsg("kdb set error without tx", tx.Write(b, nil))
	} else {
		return ErrPipeWithMsg("kdb set error with tx", k.db.Write(b, nil))
	}
}

func (k *KDB) exist(tx *leveldb.Transaction, name []byte) (bool, error) {
	if tx != nil {
		b, err := tx.Has(name, nil)
		return b, ErrPipeWithMsg("kdb exist error without tx", err)
	}
	b, err := k.db.Has(name, nil)
	return b, ErrPipeWithMsg("kdb exist error with tx", err)
}

func (k *KDB) del(tx *leveldb.Transaction, keys ... []byte) error {
	b := &leveldb.Batch{}
	for _, k := range keys {
		b.Delete(k)
	}

	if tx != nil {
		return ErrPipeWithMsg("kdb del error without tx", tx.Write(b, nil))
	} else {
		return ErrPipeWithMsg("kdb del error with tx", k.db.Write(b, nil))
	}

}

func (k *KDB) get(tx *leveldb.Transaction, key []byte) ([]byte, error) {
	if tx != nil {
		val, err := tx.Get(key, nil)
		return val, ErrPipeWithMsg("kdb get error without tx", err)
	}
	val, err := k.db.Get(key, nil)
	return val, ErrPipeWithMsg("kdb get error with tx", err)
}

// 扫描全部
func (k *KDB) ScanAll(fn func(key, value []byte) error) error {
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
func (k *KDB) scanWithPrefix(txn *leveldb.Transaction, isReverse bool, prefix []byte, fn func(key, value []byte) error) error {
	errMsg := "kdb scanWithPrefix error"

	iter := k.db.NewIterator(util.BytesPrefix(prefix), nil)
	if txn != nil {
		iter = txn.NewIterator(util.BytesPrefix(prefix), nil)
	}

	return ErrPipeWithMsg(errMsg, func() error {
		if isReverse && iter.Last() {
			for iter.Prev() {
				err := fn(bytes.TrimPrefix(iter.Key(), prefix), iter.Value())
				if err == Stop {
					break
				}

				if err != nil {
					return err
				}
			}
		}

		if !isReverse && iter.First() {
			for iter.Next() {
				err := fn(bytes.TrimPrefix(iter.Key(), prefix), iter.Value())
				if err == Stop {
					break
				}

				if err != nil {
					return err
				}
			}
		}

		iter.Release()
		return iter.Error()
	}())
}

func (k *KDB) drop(txn *leveldb.Transaction, prefix ... []byte) error {
	errMsg := "kdb drop error"
	for _, name := range prefix {
		err1 := k.scanWithPrefix(txn, false, name, func(key, value []byte) error {
			return k.del(txn, key)
		})

		pp, err := k.getPrefix(txn, name)
		if err := ErrPipeWithMsg(errMsg, err1, err, k.del(txn, WithPrefix(name)), k.set(txn, KV{Key: append(PrefixBk, name...), Value: pp})); err != nil {
			return err
		}
	}
	return nil
}

// 根据前缀扫描数据数量
func (k *KDB) sizeof(prefix []byte) (m int, err error) {
	errMsg := "kdb sizeof error"
	size, err := k.db.SizeOf([]util.Range{*util.BytesPrefix(prefix)})
	return int(size.Sum()), ErrPipeWithMsg(errMsg, err)
}
