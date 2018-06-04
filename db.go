package kdb

import (
	"bytes"
	"github.com/dgraph-io/badger"
	"github.com/kooksee/kdb/consts"
	"regexp"
	"io"
)

type KDB struct {
	db *badger.DB

	hmap map[string]*KHash
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
			panic(Errs("badger数据库启动失败", err.Error()))
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

func (k *KDB) KHash(name string) (*KHash, error) {
	if _, ok := k.hmap[name]; !ok {
		h, err := NewKHash(name, k)
		if err != nil {
			return nil, err
		}
		k.hmap[name] = h
	}
	return k.hmap[name], nil
}

func (k *KDB) Close() error {
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
	_, err := k.get(txn, key)
	if err == badger.ErrKeyNotFound {
		return false, err
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (k *KDB) get(txn *badger.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	return item.Value()
}

func (k *KDB) recordPrefix(prefix []byte) error {
	return k.db.Update(func(txn *badger.Txn) error {
		return txn.Set(append(consts.DbTypePrefix, prefix...), prefix)
	})
}

func (k *KDB) KHashNames() (names []string, err error) {
	hashPrefix := []byte(F("%s%s", consts.KHASH, consts.Separator))
	return names, k.db.View(func(txn *badger.Txn) error {
		return k.RangeWithPrefix(txn, append(consts.DbTypePrefix, hashPrefix...), func(key, value []byte) error {
			names = append(names, string(bytes.TrimSuffix(key, []byte{consts.Separator})))
			return nil
		})
	})
}

func (k *KDB) set(txn *badger.Txn, key, value []byte) error {
	return k.mSet(txn, &KV{Key: key, Value: value})
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
func (k *KDB) ScanFilter(txn *badger.Txn, prefix []byte, filter func(key, value []byte) bool, fn func(key, value []byte) error) error {
	return k.RangeWithPrefix(txn, prefix, func(key, value []byte) error {
		if filter(key, value) {
			return fn(key, value)
		}
		return nil
	})
}

// 根据key pattern正则表达式匹配扫描
func (k *KDB) ScanPattern(txn *badger.Txn, prefix []byte, pattern string, fn func(key, value []byte) error) error {
	r, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	return k.RangeWithPrefix(txn, prefix, func(key, value []byte) error {
		if r.Match(key) {
			return fn(key, value)
		}
		return nil
	})
}

func (k *KDB) ReverseWithPrefix(txn *badger.Txn, prefix []byte, fn func(key, value []byte) error) error {
	return k.Reverse(txn, prefix, append(prefix, consts.MAXBYTE), func(key, value []byte) error {
		if bytes.HasPrefix(key, prefix) {
			return fn(bytes.TrimPrefix(key, prefix), value)
		}
		return nil
	})
}

func (k *KDB) RangeWithPrefix(txn *badger.Txn, prefix []byte, fn func(key, value []byte) error) error {
	return k.Range(txn, prefix, append(prefix, consts.MAXBYTE), func(key, value []byte) error {
		if bytes.HasPrefix(key, prefix) {
			return fn(bytes.TrimPrefix(key, prefix), value)
		}
		return nil
	})
}

func (k *KDB) Range(txn *badger.Txn, start, end []byte, fn func(key, value []byte) error) error {
	return k.scan(txn, false, start, end, fn)
}

func (k *KDB) Reverse(txn *badger.Txn, start, end []byte, fn func(key, value []byte) error) error {
	return k.scan(txn, true, start, end, fn)
}

func (k *KDB) scanIter(txn *badger.Txn, isReverse bool, start, end []byte, values chan *KV) {
	values <- ErrKV(k.scan(txn, isReverse, start, end, func(key, value []byte) error {
		values <- NewKV(key, value)
		return nil
	}))
}

// 范围扫描
func (k *KDB) scan(txn *badger.Txn, isReverse bool, start, end []byte, fn func(key, value []byte) error) error {

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

	for ; iter.Valid(); iter.Next() {

		k := iter.Item().Key()
		if !Between(k, start, end) {
			break
		}

		v, err := iter.Item().Value()
		if err != nil {
			return err
		}

		err = fn(k, v)
		if err == io.EOF {
			break
		}
		return err
	}

	return nil
}

func (k *KDB) PopRandom(txn *badger.Txn, prefix []byte, n int, fn func(key, value []byte) error) error {
	return k.ScanRandom(txn, prefix, n, func(key, value []byte) error {
		if err := fn(key, value); err != nil {
			return err
		}

		if err := txn.Delete(append(prefix, key...)); err != nil {
			return err
		}

		return nil
	})
}

func (k *KDB) Pop(txn *badger.Txn, prefix []byte, fn func(key, value []byte) error) error {
	return k.ReverseWithPrefix(txn, prefix, func(key, value []byte) error {

		if err := fn(key, value); err != nil {
			return err
		}

		if err := txn.Delete(append(prefix, key...)); err != nil {
			return err
		}

		return nil
	})
}

func (k *KDB) PopN(txn *badger.Txn, prefix []byte, n int, fn func(key, value []byte) error) error {
	return k.ReverseWithPrefix(txn, prefix, func(key, value []byte) error {

		if n < 1 {
			return io.EOF
		}

		if err := fn(key, value); err != nil {
			return err
		}

		if err := txn.Delete(append(prefix, key...)); err != nil {
			return err
		}

		n--

		return nil
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
func (k *KDB) ScanRandom(txn *badger.Txn, prefix []byte, count int, fn func(key, value []byte) error) error {

	cnt, err := k.Len(txn, prefix)
	if err != nil {
		return err
	}

	if cnt < count {
		return k.RangeWithPrefix(txn, prefix, fn)
	}

	rmd := GenRandom(0, cnt, count)
	m := 0
	return k.RangeWithPrefix(txn, prefix, func(key, value []byte) error {
		var val error
		if rmd[m] {
			val = fn(key, value)
		}

		m++
		return val
	})
}

// 根据前缀扫描数据数量
func (k *KDB) Len(txn *badger.Txn, prefix []byte) (m int, err error) {
	return m, k.RangeWithPrefix(txn, prefix, func(_, _ []byte) error {
		m++
		return nil
	})
}
