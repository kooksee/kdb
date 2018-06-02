package kdb

import (
	"bytes"
	"regexp"
	"github.com/dgraph-io/badger"
)

type KDB struct {
	db *badger.DB

	kdir string
	hmap map[string]*KHash
	lmap map[string]*KList
}

// InitKdb 初始化数据库
func InitKdb(path string) {

	if !IsFileExist(path) {
		panic("kdb数据存储目录不存在")
	}

	once.Do(func() {
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

func (k *KDB) KHPrefix(name []byte) []byte {
	return []byte(f("h/%s/", name))
}

func (k *KDB) KLPrefix(name []byte) []byte {
	return []byte(f("l/%s/", name))
}

func (k *KDB) K(prefix, key []byte) []byte {
	return BConcat(prefix, key)
}

func (k *KDB) exist(txn *badger.Txn, key []byte) (bool, error) {
	res, err := k.get(txn, key)
	if err != nil {
		return false, err
	}

	if res != nil {
		return false, nil
	}

	return true, nil
}

func (k *KDB) get(txn *badger.Txn, key []byte) ([]byte, error) {
	vals, err := k.mGet(txn, key)
	return vals[0], err
}

// MGet 取多个值
func (k *KDB) mGet(txn *badger.Txn, keys ... []byte) (vals [][]byte, err error) {
	for i, key := range keys {

		item, err := txn.Get(key)
		if err != nil {
			return nil, err
		}

		if err == badger.ErrKeyNotFound {
			vals[i] = nil
			continue
		}

		if err != nil {
			return nil, err
		}

		vals[i], err = item.Value()
		if err != nil {
			return nil, err
		}
	}
	return vals, nil
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

// 范围扫描
func (k *KDB) ScanRange(txn *badger.Txn, isReverse bool, min, max []byte, fn func(i int, key, value []byte) bool) error {

	opt := badger.DefaultIteratorOptions
	opt.Reverse = isReverse

	iter := txn.NewIterator(opt)
	defer iter.Close()

	if isReverse {
		if len(max) == 0 {
			iter.Rewind()
		} else {
			iter.Seek(max)
		}
	} else {
		if len(min) == 0 {
			iter.Rewind()
		} else {
			iter.Seek(min)
		}
	}

	for i := 0; iter.Valid(); iter.Next() {

		v, err := iter.Item().Value()
		if err != nil {
			return err
		}

		k := iter.Item().Key()
		if !between(k, min, max) {
			break
		}

		if fn(i, k, v) {
			i++
			continue
		}
		break
	}

	return nil
}

func (k *KDB) PopN(txn *badger.Txn, isReverse bool, min, max []byte, n int, fn func(i int, key, value []byte)) error {

	opt := badger.DefaultIteratorOptions
	opt.Reverse = isReverse

	iter := txn.NewIterator(opt)
	defer iter.Close()

	if isReverse {
		if len(max) == 0 {
			iter.Rewind()
		} else {
			iter.Seek(max)
		}
	} else {
		if len(min) == 0 {
			iter.Rewind()
		} else {
			iter.Seek(min)
		}
	}

	for i := 0; iter.Valid() && i < n; iter.Next() {

		v, err := iter.Item().Value()
		if err != nil {
			return err
		}

		k := iter.Item().Key()
		if err := txn.Delete(k); err != nil {
			return err
		}

		if !between(k, min, max) {
			break
		}

		fn(i, k, v)

		i++

		return nil
	}

	return nil
}

func (k *KDB) PopRandom(txn *badger.Txn, prefix []byte, n int, fn func(i int, key, value []byte)) error {

	opt := badger.DefaultIteratorOptions
	opt.Reverse = false

	iter := txn.NewIterator(opt)
	defer iter.Close()

	cnt, err := k.Len(txn, prefix)
	if err != nil {
		return err
	}

	if cnt < n {
		return k.Scan(txn, prefix, 0, func(i int, key, value []byte) bool {
			fn(i, bytes.TrimPrefix(key, prefix), value)
			return true
		})
	}

	rmd := genRandom(0, cnt, n)
	for i := 0; iter.Valid(); iter.Next() {
		if !rmd[i] || i >= n {
			continue
		}

		v, err := iter.Item().Value()
		if err != nil {
			return err
		}

		k := iter.Item().Key()
		if err := txn.Delete(k); err != nil {
			return err
		}

		fn(i, bytes.TrimPrefix(k, prefix), v)
		i++
	}

	return nil
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

// Scan 根据前缀扫描,n=0的时候全扫描
func (k *KDB) Scan(txn *badger.Txn, prefix []byte, n int, fn func(i int, key, value []byte) bool) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	iter.Seek(prefix)
	for i := 0; iter.ValidForPrefix(prefix) && i < n; iter.Next() {
		v, err := iter.Item().Value()
		if err != nil {
			return err
		}

		if !fn(i, bytes.TrimPrefix(iter.Item().Key(), prefix), v) {
			return nil
		}

		i++
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
		return k.Scan(txn, prefix, count, fn)
	}

	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	rmd := genRandom(0, cnt, count)
	iter.Seek(prefix)
	for i := 0; iter.ValidForPrefix(prefix); iter.Next() {
		v, err := iter.Item().Value()
		if err != nil {
			return err
		}

		if rmd[i] && !fn(i, bytes.TrimPrefix(iter.Item().Key(), prefix), v) {
			return nil
		}

		i++
	}
	return nil
}

// 根据key pattern正则表达式匹配扫描
func (k *KDB) ScanPattern(txn *badger.Txn, pattern string, prefix []byte, fn func(i int, key, value []byte) bool) error {

	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	r, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	iter.Seek(prefix)
	for i := 0; iter.ValidForPrefix(prefix); iter.Next() {
		v, err := iter.Item().Value()
		if err != nil {
			return err
		}

		k := bytes.TrimPrefix(iter.Item().Key(), prefix)
		if r.Match(k) && !fn(i, k, v) {
			return nil
		}

		i++
	}
	return nil
}

// 根据前缀扫描数据数量
func (k *KDB) Len(txn *badger.Txn, prefix []byte) (int, error) {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	i := 0
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		if _, err := iter.Item().Value(); err != nil {
			return 0, err
		}
		i++
	}
	return i, nil
}
