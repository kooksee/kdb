package kdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/json-iterator/go"
)

func (k *KHash) set(txn *leveldb.Transaction, kv ... KV) error {
	return ErrPipeWithMsg("khash set error", k.db.set(txn, KVMap(kv, func(_ int, kv KV) KV {
		kv.Key = k.K(kv.Key)
		return kv
	})...))
}

func (k *KHash) del(txn *leveldb.Transaction, key ... []byte) error {
	return ErrPipeWithMsg("khash del error", k.db.del(txn, BMap(key, func(_ int, k1 []byte) []byte {
		return k.K(k1)
	})...))
}

func (k *KHash) get(txn *leveldb.Transaction, key []byte) ([]byte, error) {
	val, err := k.db.get(txn, k.K(key))
	return val, ErrPipeWithMsg("khash get error", err)
}

func (k *KHash) getJson(txn *leveldb.Transaction, key []byte, path ...interface{}) (jsoniter.Any, error) {
	val, err := k.db.get(txn, k.K(key))
	return json.Get(val, path...), ErrPipeWithMsg("khash getJson error", err)
}

func (k *KHash) exist(txn *leveldb.Transaction, key []byte) (bool, error) {
	val, err := k.db.exist(txn, k.K(key))
	return val, ErrPipeWithMsg("khash exist error", err)
}

func (k *KHash) _map(txn *leveldb.Transaction, fn func(key, value []byte) ([]byte, error)) error {
	return k.db.scanWithPrefix(txn, false, k.Prefix(), func(key, value []byte) error {
		val, err := fn(key, value)
		return ErrPipeWithMsg("khash map error", err, k.set(txn, KV{Key: key, Value: val}))
	})
}

func (k *KHash) union(txn *leveldb.Transaction, others ... []byte) error {
	if txn != nil {
		panic(Errs("khash union error", "txn is nil"))
	}

	b := &leveldb.Batch{}
	for _, o := range others {
		kh, err := k.db.getPrefix(txn, o)
		if err := ErrPipeWithMsg("khash union range error", err, k.db.scanWithPrefix(txn, false, o, func(key, value []byte) error {
			b.Put(k.K(key), value)
			b.Delete(append(kh, key...))
			return nil
		})); err != nil {
			return err
		}
	}
	return ErrPipeWithMsg("khash union error", txn.Write(b, nil))
}

func (k *KHash) getSet(txn *leveldb.Transaction, key, value []byte) (val []byte, err error) {
	val, err = k.get(txn, key)
	return val, ErrPipeWithMsg("khash getSet error", err, k.set(txn, KV{Key: key, Value: value}))
}

func (k *KHash) popRandom(txn *leveldb.Transaction, n int, fn func(key, value []byte) error) error {
	return k.scanRandom(txn, n, func(k1, v1 []byte) error {
		return ErrPipeWithMsg("khash popRandom error", fn(k1, v1), k.del(txn, k1))
	})
}

func (k *KHash) pop(txn *leveldb.Transaction, fn func(key, value []byte) error) error {
	return k.db.scanWithPrefix(txn, true, k.Prefix(), func(key, value []byte) error {
		return ErrPipeWithMsg("khash pop error", fn(key, value), k.del(txn, key))
	})
}

func (k *KHash) popN(txn *leveldb.Transaction, n int, fn func(key, value []byte) error) error {
	return ErrPipeWithMsg("khash popn error", k.pop(txn, func(key, value []byte) error {
		if n < 1 {
			return Stop
		}
		n--
		return fn(key, value)
	}))
}

// ScanRandom 根据前缀随机扫描
func (k *KHash) scanRandom(txn *leveldb.Transaction, count int, fn func(key, value []byte) error) error {
	errmsg := "khash scanRandom error"
	l, err := k.len()
	if l < count {
		return ErrPipeWithMsg(errmsg, err, k.db.scanWithPrefix(txn, true, k.Prefix(), fn))
	}

	m := -1
	rmd := GenRandom(0, l, count)
	return ErrPipeWithMsg(errmsg, k.db.scanWithPrefix(txn, true, k.Prefix(), func(key, value []byte) error {
		m++
		if rmd[m] {
			err = fn(key, value)
		}
		return err
	}))
}

func (k *KHash) len() (int, error) {
	errMsg := "khash len error"
	l, err := k.db.sizeof(k.Prefix())
	return l, ErrPipeWithMsg(errMsg, err)
}
