package kdb

import (
	"bytes"
	"github.com/dgraph-io/badger"
	"github.com/kooksee/kdb/consts"
	"regexp"
	"os"
	"github.com/inconshreveable/log15"
	"errors"
)

var DbTypePrefix = []byte(F("%s%s%s%s", consts.DbNamePrefix, consts.Separator, "type", consts.Separator))
var HashPrefix = []byte(F("%s%s", consts.KHASH, consts.Separator))
var Stop = errors.New("STOP")

type KDB struct {
	db *badger.DB

	hmap map[string]*KHash
	l    log15.Logger
}

// InitKdb 初始化数据库
func InitKdb(path string) {
	once.Do(func() {

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

func (k *KDB) InitLog(l log15.Logger) {
	if l != nil {
		k.l = l.New("package", "kdb")
	} else {
		k.l = log15.New("package", "kdb")
		ll, err := log15.LvlFromString("debug")
		if err != nil {
			panic(err.Error())
		}
		k.l.SetHandler(log15.LvlFilterHandler(ll, log15.StreamHandler(os.Stdout, log15.TerminalFormat())))
	}
}

func GetLog() log15.Logger {
	if GetKdb().l == nil {
		panic("please init sp2p log")
	}
	return GetKdb().l
}

func (k *KDB) KHashExist(name string) bool {
	var (
		err error
		b   bool
	)

	key := append(DbTypePrefix, []byte(F("%s%s%s%s", consts.KHASH, consts.Separator, name, consts.Separator))...)
	GetLog().Debug("debug", "key", string(key))

	if err := k.db.View(func(txn *badger.Txn) error {
		b, err = k.exist(txn, key)
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
		return txn.Set(append(DbTypePrefix, prefix...), prefix)
	})
}

func (k *KDB) KHashNames() []string {
	names := make([]string, 0)

	if err := k.db.View(func(txn *badger.Txn) error {
		return k.RangeWithPrefix(txn, append(DbTypePrefix, HashPrefix...), func(key, value []byte) error {
			names = append(names, string(bytes.TrimSuffix(key, []byte{consts.Separator})))
			return nil
		})
	}); err != nil {
		GetLog().Error("KHashNames error", "err", err.Error())
	}
	return names
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
		if err == Stop {
			break
		}
		return err
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
func (k *KDB) Len(prefix []byte) (m int) {
	if err := k.db.View(func(txn *badger.Txn) error {
		return k.RangeWithPrefix(txn, prefix, func(_, _ []byte) error {
			m++
			return nil
		})
	}); err != nil {
		GetLog().Error("Len error", "err", err.Error())
		return 0
	}
	return m
}
