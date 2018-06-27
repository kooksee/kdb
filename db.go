package kdb

import (
	"bytes"
	"github.com/dgraph-io/badger"
	"github.com/kooksee/kdb/consts"
	"os"
	"github.com/inconshreveable/log15"
	"errors"
	"path/filepath"
)

var dbTypePrefix = []byte(F("%s%s%s%s", consts.DbNamePrefix, consts.Separator, "type", consts.Separator))
var hashPrefix = []byte(F("%s%s", consts.KHASH, consts.Separator))
var Stop = errors.New("STOP")

var l1 log15.Logger

type KDB struct {
	db   *badger.DB
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

		opts := badger.DefaultOptions
		opts.Dir = path
		opts.ValueDir = path

		db, err := badger.Open(opts)
		if err != nil {
			panic(Errs("badger数据库启动失败", err.Error()))
		}
		kdb = &KDB{db: db, hmap: make(map[string]*KHash)}
	})
}

// GetKdb 得到kdb实例
func GetKdb() *KDB {
	if kdb == nil {
		panic("请初始化kdb")
	}
	return kdb
}

func InitLog(l ... log15.Logger) {
	if len(l) > 0 {
		l1 = l[0].New("module", "kdb")
		return
	}

	l1 = log15.New("module", "kdb")
	ll, err := log15.LvlFromString("debug")
	if err != nil {
		panic(Errs("init log error", err.Error()))
	}
	l1.SetHandler(log15.LvlFilterHandler(ll, log15.StreamHandler(os.Stdout, log15.TerminalFormat())))
}

func GetLog() log15.Logger {
	if l1 == nil {
		panic("please init sp2p log")
	}
	return l1
}

func (k *KDB) KHashExist(name string) bool {
	var (
		err error
		b   bool
	)

	if err := k.db.View(func(txn *badger.Txn) error {
		b, err = k.exist(txn, append(dbTypePrefix, []byte(F("%s%s%s%s", consts.KHASH, consts.Separator, name, consts.Separator))...))
		return err
	}); err != nil {
		GetLog().Error("KHashExist error", "err", err.Error())
		b = false
	}
	return b
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

func (k *KDB) GetWithTx(fn func(txn *badger.Txn) error) error {
	return k.db.View(func(txn1 *badger.Txn) error {
		return fn(txn1)
	})
}

func (k *KDB) UpdateWithTx(fn func(txn *badger.Txn) error) error {
	return k.db.Update(func(txn1 *badger.Txn) error {
		return fn(txn1)
	})
}

func (k *KDB) exist(txn *badger.Txn, key []byte) (bool, error) {
	_, err := k.get(txn, key)
	if err == badger.ErrKeyNotFound {
		return false, nil
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
		return txn.Set(append(dbTypePrefix, prefix...), []byte("ok"))
	})
}

//func (k *KDB) KHashNames() (names []string) {
//	if err := k.db.View(func(txn *badger.Txn) error {
//		return k.RangeWithPrefix(txn, append(dbTypePrefix, hashPrefix...), func(key, value []byte) error {
//			names = append(names, string(bytes.TrimSuffix(key, []byte(consts.Separator))))
//			return nil
//		})
//	}); err != nil {
//		GetLog().Error("KHashNames error", "err", err.Error())
//	}
//
//	return
//}

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
//func (k *KDB) ScanFilter(txn *badger.Txn, prefix []byte, filter func(key, value []byte) bool, fn func(fk, fv []byte) error) error {
//	return k.RangeWithPrefix(txn, prefix, func(k1, v1 []byte) error {
//		if filter(k1, v1) {
//			return fn(k1, v1)
//		}
//		return nil
//	})
//}

// 根据key pattern正则表达式匹配扫描
//func (k *KDB) ScanPattern(txn *badger.Txn, prefix []byte, pattern string, fn func(key, value []byte) error) error {
//	r, err := regexp.Compile(pattern)
//	if err != nil {
//		return err
//	}
//
//	return k.RangeWithPrefix(txn, prefix, func(k1, v1 []byte) error {
//		if r.Match(k1) {
//			return fn(k1, v1)
//		}
//		return nil
//	})
//}

func (k *KDB) ReverseWithPrefix(txn *badger.Txn, prefix []byte, fn func(key, value []byte) error) error {
	return k.Reverse(txn, prefix, append(prefix, consts.MAXBYTE), func(k1, v1 []byte) error {
		k2 := bytes.TrimPrefix(k1, prefix)
		return fn(k2, v1)
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

// 扫描全部
func (k *KDB) ScanAll(fn func(key, value []byte) error) error {
	return k.GetWithTx(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions

		iter := txn.NewIterator(opt)
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {

			k := iter.Item().Key()

			v, err := iter.Item().Value()
			if err != nil {
				GetLog().Error("ScanAll error", "err", err.Error())
				return err
			}

			err = fn(k, v)
			if err == Stop {
				break
			}

			if err != nil {
				GetLog().Error("ScanAll callback error", "err", err.Error())
				return err
			}
		}
		return nil
	})
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

		if k == nil || v == nil || len(k) == 0 || len(v) == 0 {
			break
		}

		err = fn(k, v)
		if err == Stop {
			break
		}
		if err != nil {
			return err
		}
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

// 根据前缀扫描数据数量
//func (k *KDB) Len(prefix []byte) (m int) {
//
//	if err := k.db.View(func(txn *badger.Txn) error {
//		return k.RangeWithPrefix(txn, prefix, func(_, _ []byte) error {
//			m++
//			return nil
//		})
//	}); err != nil {
//		GetLog().Error("kdb len error", "err", err.Error())
//	}
//
//	return
//}
