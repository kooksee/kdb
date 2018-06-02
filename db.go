package kdb

import (
	"bytes"
	"github.com/dgraph-io/badger"
	"errors"
	"github.com/kooksee/kdb/consts"
	"regexp"
)

type KDB struct {
	db *badger.DB

	kdir string
	hmap map[string]*KHash
	lmap map[string]*KList
}

// InitKdb 初始化数据库
func InitKdb(path string) {
	once.Do(func() {

		if !IsFileExist(path) {
			panic("kdb数据存储目录不存在")
		}

		opts := badger.DefaultOptions
		opts.Dir = path
		opts.ValueDir = path

		db, err := badger.Open(opts)
		if err != nil {
			panic(Errs("badger启动数据库失败", err.Error()))
		}
		kdb = &KDB{db: db}
	})
}

// GetKdb 得到kdb实例
func GetKdb() *KDB {
	if kdb == nil {
		panic("请初始化kdb")
	}
	return kdb
}

func (k *KDB) KHash(name string) *KHash {
	if _, ok := k.hmap[name]; !ok {
		k.hmap[name] = NewKHash(name, k)
	}

	return k.hmap[name]
}

func (k *KDB) KList(name string) *KList {
	if _, ok := k.lmap[name]; !ok {
		k.lmap[name] = NewKList(name, k)
	}

	return k.lmap[name]
}

func (k *KDB) close() error {
	return k.db.Close()
}

func (k *KDB) GetWithTx(fn func(txn *badger.Txn) error) error {
	return k.db.View(func(txn *badger.Txn) error {
		return fn(txn)
	})
}

func (k *KDB) UpdateWithTx(fn func(txn *badger.Txn) error) error {
	return k.db.Update(func(txn *badger.Txn) error {
		return fn(txn)
	})
}

func (k *KDB) exist(txn *badger.Txn, key []byte) (bool, error) {
	res, err := k.get(txn, key)
	if err != nil {
		return false, err
	}

	if res == nil {
		return false, nil
	}

	return true, nil
}

func (k *KDB) get(txn *badger.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	if err == badger.ErrKeyNotFound {
		return nil, errors.New(F("找不到％s", key))
	}

	if err != nil {
		return nil, err
	}

	return item.Value()
}

func (k *KDB) set(txn *badger.Txn, key, value []byte) error {
	return k.mSet(txn, &KV{key, value})
}

func (k *KDB) mSet(txn *badger.Txn, kvs ... *KV) error {
	for _, kv := range kvs {
		if err := txn.Set(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

func (k *KDB) mDel(txn *badger.Txn, ks ... []byte) error {
	for _, k := range ks {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// 根据key pattern正则表达式匹配扫描
func (k *KDB) ScanFilter(txn *badger.Txn, prefix []byte, filter func(i int, key, value []byte) bool, fn func(i int, key, value []byte) bool) error {
	return k.PrefixRange(txn, prefix, func(i int, key, value []byte) bool {
		if filter(i, key, value) {
			return fn(i, key, value)
		}
		return true
	})
}

// 根据key pattern正则表达式匹配扫描
func (k *KDB) ScanPattern(txn *badger.Txn, prefix []byte, pattern string, fn func(i int, key, value []byte) bool) error {
	r, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	return k.PrefixRange(txn, prefix, func(i int, key, value []byte) bool {
		if r.Match(key) {
			return fn(i, key, value)
		}
		return true
	})
}

func (k *KDB) PrefixReverse(txn *badger.Txn, prefix []byte, fn func(i int, key, value []byte) bool) error {
	return k.Range(txn, prefix, append(prefix, consts.MAXBYTE), func(i int, key, value []byte) bool {
		if bytes.HasPrefix(key, prefix) {
			return fn(i, bytes.TrimPrefix(key, prefix), value)
		}
		return true
	})
}

func (k *KDB) PrefixRange(txn *badger.Txn, prefix []byte, fn func(i int, key, value []byte) bool) error {
	return k.Range(txn, prefix, append(prefix, consts.MAXBYTE), func(i int, key, value []byte) bool {
		if bytes.HasPrefix(key, prefix) {
			return fn(i, bytes.TrimPrefix(key, prefix), value)
		}
		return true
	})
}

func (k *KDB) Range(txn *badger.Txn, start, end []byte, fn func(i int, key, value []byte) bool) error {
	return k.scanRange(txn, false, start, end, fn)
}

func (k *KDB) Reverse(txn *badger.Txn, start, end []byte, fn func(i int, key, value []byte) bool) error {
	return k.scanRange(txn, true, start, end, fn)
}

// 范围扫描
func (k *KDB) scanRange(txn *badger.Txn, isReverse bool, start, end []byte, fn func(i int, key, value []byte) bool) error {

	opt := badger.DefaultIteratorOptions
	opt.Reverse = isReverse

	iter := txn.NewIterator(opt)
	defer iter.Close()

	if isReverse {
		if len(end) == 0 {
			iter.Rewind()
		} else {
			iter.Seek(end)
		}
	} else {
		if len(start) == 0 {
			iter.Rewind()
		} else {
			iter.Seek(start)
		}
	}

	for i := 0; iter.Valid(); iter.Next() {

		k := iter.Item().Key()
		if !Between(k, start, end) {
			break
		}

		v, err := iter.Item().Value()
		if err != nil {
			return err
		}

		if !fn(i, k, v) {
			break
		}

		i++
	}

	return nil
}

func (k *KDB) PopRandom(txn *badger.Txn, prefix []byte, n int, fn func(i int, key, value []byte) bool) error {
	return k.ScanRandom(txn, prefix, n, func(i int, key, value []byte) bool {
		if !fn(i, key, value) {
			return false
		}

		if err := txn.Delete(append(prefix, key...)); err != nil {
			return false
		}

		return true
	})
}

func (k *KDB) Drop(txn *badger.Txn, prefix ... []byte) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for _, name := range prefix {
		for iter.Seek(name); iter.ValidForPrefix(name); iter.Next() {
			if err := txn.Delete(iter.Item().Key()); err != nil {
				return err
			}
		}
	}

	return nil
}

// ScanRandom 根据前缀随机扫描
func (k *KDB) ScanRandom(txn *badger.Txn, prefix []byte, count int, fn func(i int, key, value []byte) bool) error {

	cnt, err := k.Len(txn, prefix)
	if err != nil {
		return err
	}

	if cnt < count {
		return k.PrefixRange(txn, prefix, fn)
	}

	rmd := GenRandom(0, cnt, count)
	return k.PrefixRange(txn, prefix, func(i int, key, value []byte) bool {
		if rmd[i] {
			return fn(i, key, value)
		}
		return true
	})
}

// 根据前缀扫描数据数量
func (k *KDB) Len(txn *badger.Txn, prefix []byte) (m int, err error) {
	return m, k.PrefixRange(txn, prefix, func(i int, _, _ []byte) bool {
		m = i
		return true
	})
}
