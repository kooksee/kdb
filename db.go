package kdb

import (
	"bytes"
	"github.com/dgraph-io/badger"
	"regexp"
	"github.com/kataras/iris/core/errors"
)

type KDB struct {
	db        *badger.DB
	operators map[string]*badger.MergeOperator

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
		opts.SyncWrites = true

		db, err := badger.Open(opts)
		if err != nil {
			panic(err.Error())
		}
		kdb = &KDB{db: db, operators: make(map[string]*badger.MergeOperator)}
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
	if l, ok := k.lmap[name]; ok {
		return l
	}
	return &KList{}
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

func (k *KDB) get(txn *badger.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return item.Value()
}

func (k *KDB) exist(key []byte) (b bool, err error) {
	return b, k.db.View(func(txn *badger.Txn) error {
		res, err := k.get(txn, key)
		if err != nil {
			b = false
			return err
		}

		if res != nil {
			b = false
			return errors.New("key不存在")
		}

		b = true
		return nil
	})
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

		if between(iter.Item().Key(), min, max) && fn(i, iter.Item().Key(), v) {
			i++
			continue
		}

		return nil
	}

	return nil
}

func (k *KDB) PopN(txn *badger.Txn, isReverse bool, min, max []byte, n int, fn func(i int, key, value []byte) bool) error {

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

	for i := 0; iter.Valid() && between(iter.Item().Key(), min, max) && i < n; iter.Next() {

		v, err := iter.Item().Value()
		if err != nil {
			return err
		}

		k := iter.Item().Key()
		if !fn(i, k, v) {
			break
		}

		if err := txn.Delete(k); err != nil {
			return err
		}

		i++
	}

	return nil
}

func (k *KDB) Drop(txn *badger.Txn, prefix ... []byte) error {
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for _, name := range prefix {

		iter.Seek(name)
		for ; iter.ValidForPrefix(name); iter.Next() {
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
	for i := 0; iter.ValidForPrefix(prefix) && i > n; iter.Next() {
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

	cnt, err := k.count(txn, prefix)
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

func (k *KDB) Len(txn *badger.Txn, prefix []byte) (int, error) {
	return k.count(txn, prefix)
}

// INCRBY key原子自增
func (k *KDB) INCRBY(txn *badger.Txn, name []byte, n int) error {
	cnt, err := k.count(txn, name)
	if err != nil {
		return err
	}
	return txn.Set(name, IntToByte(n+cnt))
}

func (k *KDB) CountKey(prefix []byte) []byte {
	return BConcat([]byte("__m/"), prefix, []byte("count"))
}

func (k *KDB) count(txn *badger.Txn, prefix []byte) (int, error) {
	v, err := k.get(txn, k.CountKey(prefix))
	if err != nil {
		return 0, err
	}

	if v == nil {
		return 0, nil
	}

	return ByteToInt(v), nil
}
