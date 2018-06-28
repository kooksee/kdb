package kdb

import (
	"github.com/kooksee/kdb/consts"
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
				panic(F("Could not create directory %v. %v", path, err))
			}
		}

		db, err := leveldb.OpenFile(path, nil)
		if err != nil {
			panic(Errs("数据库启动失败", err.Error()))
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
	return k.db.Has(append([]byte(consts.Prefix), name...), nil)
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

func (k *KDB) KHash(name string) *KHash {
	if _, ok := k.hmap[name]; !ok {
		k.hmap[name] = NewKHash(name, k)
	}
	return k.hmap[name]
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

	return txn.Commit()
}

func (k *KDB) getPrefix(tx *leveldb.Transaction, prefix []byte) ([]byte, error) {
	errMsg := "kdb getPrefix error"

	if tx == nil {
		val, err := k.db.Get(consts.WithPrefix(prefix), nil)
		return val, ErrPipeWithMsg(errMsg, err)
	}

	val, err := tx.Get(consts.WithPrefix(prefix), nil)
	return val, ErrPipeWithMsg(errMsg, err)
}

// 存储并得到前缀
func (k *KDB) recordPrefix(prefix []byte) (px []byte, err error) {
	errMsg := "kdb recordPrefix error"
	return px, ErrPipeWithMsg(errMsg, k.WithTxn(func(tx *leveldb.Transaction) error {
		key := consts.WithPrefix(prefix)
		ext, err := tx.Has(key, nil)
		if err != nil {
			return err
		}
		// 存在就不再存储
		if ext {
			return nil
		}

		l, err := k.sizeof(tx, []byte(consts.Prefix))
		if err != err {
			return err
		}

		px = append(big.NewInt(int64(l)).Bytes(), consts.DataPrefix...)
		return tx.Put(key, px, nil)
	}))
}

func (k *KDB) KHashNames() (names []string, err error) {
	errMsg := "kdb KHashNames error"
	return names, ErrPipeWithMsg(errMsg, k.WithTxn(func(tx *leveldb.Transaction) error {
		return k.scanWithPrefix(tx, false, consts.Prefix, func(key, value []byte) error {
			names = append(names, string(bytes.TrimSuffix(key, consts.Prefix)))
			return nil
		})
	}))
}

func (k *KDB) set(tx *leveldb.Transaction, kv ... KV) error {
	b := &leveldb.Batch{}
	for _, i := range kv {
		b.Put(i.Key, i.Value)
	}
	return ErrPipeWithMsg("kdb set error", tx.Write(b, nil))
}

func (k *KDB) exist(tx *leveldb.Transaction, name []byte) (bool, error) {
	errMsg := "kdb exist error"
	if tx != nil {
		b, err := tx.Has(name, nil)
		return b, ErrPipeWithMsg(errMsg, err)
	}
	b, err := k.db.Has(name, nil)
	return b, ErrPipeWithMsg(errMsg, err)
}

func (k *KDB) del(tx *leveldb.Transaction, keys ... []byte) error {
	errMsg := "kdb del error"
	b := &leveldb.Batch{}
	for _, k := range keys {
		b.Delete(k)
	}
	return ErrPipeWithMsg(errMsg, tx.Write(b, nil))
}

func (k *KDB) get(tx *leveldb.Transaction, key []byte) ([]byte, error) {
	errMsg := "kdb get error"
	if tx != nil {
		val, err := tx.Get(key, nil)
		return val, ErrPipeWithMsg(errMsg, err)
	}
	val, err := k.db.Get(key, nil)
	return val, ErrPipeWithMsg(errMsg, err)
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
	return ErrPipeWithMsg(errMsg, func() error {
		for _, name := range prefix {
			if err := k.scanWithPrefix(txn, false, name, func(key, value []byte) error {
				return k.del(txn, key)
			}); err != nil {
				return err
			}
		}
		return nil
	}())
}

// 根据前缀扫描数据数量
func (k *KDB) sizeof(prefix []byte) (m int, err error) {
	errMsg := "kdb sizeof error"
	size, err := k.db.SizeOf([]util.Range{*util.BytesPrefix(prefix)})
	return int(size.Sum()), ErrPipeWithMsg(errMsg, err)
}
