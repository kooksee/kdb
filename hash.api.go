package kdb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/json-iterator/go"
	"io"
)

func (k *kHash) set(txn *leveldb.Transaction, kv ... KV) error {
	return errWithMsg("kHash set error", k.db.set(txn, kvMap(kv, func(_ int, kv KV) KV {
		kv.Key = k.k(kv.Key)
		return kv
	})...))
}

func (k *kHash) del(txn *leveldb.Transaction, key ... []byte) error {
	return errWithMsg("kHash del error", k.db.del(txn, bMap(key, func(_ int, k1 []byte) []byte {
		return k.k(k1)
	})...))
}

func (k *kHash) get(txn *leveldb.Transaction, key []byte) ([]byte, error) {
	val, err := k.db.get(txn, k.k(key))
	return val, errWithMsg("kHash get error", err)
}

func (k *kHash) getJson(txn *leveldb.Transaction, key []byte, path ...interface{}) (jsoniter.Any, error) {
	val, err := k.db.get(txn, k.k(key))
	return json.Get(val, path...), errWithMsg("kHash getJson error", err)
}

func (k *kHash) exist(txn *leveldb.Transaction, key []byte) (bool, error) {
	val, err := k.db.exist(txn, k.k(key))
	return val, errWithMsg("kHash exist error", err)
}

func (k *kHash) _map(txn *leveldb.Transaction, fn func(key, value []byte) ([]byte, error)) error {
	return k.db.scanWithPrefix(txn, false, k.getPrefix(), func(key, value []byte) error {
		val, err := fn(key, value)
		return errWithMsg("kHash map error", err, k.set(txn, KV{Key: key, Value: val}))
	})
}

func (k *kHash) union(txn *leveldb.Transaction, others ... []byte) error {
	if txn != nil {
		mustNotErr(errs("kHash union error txn is nil"))
	}

	b := &leveldb.Batch{}
	for _, o := range others {
		kh, err := k.db.getPrefix(txn, o)
		if err := errWithMsg("kHash union range error", err, k.db.scanWithPrefix(txn, false, o, func(key, value []byte) error {
			b.Put(k.k(key), value)
			b.Delete(append(kh, key...))
			return nil
		})); err != nil {
			return err
		}
	}
	return errWithMsg("kHash union error", txn.Write(b, nil))
}

func (k *kHash) getSet(txn *leveldb.Transaction, key, value []byte) (val []byte, err error) {
	val, err = k.get(txn, key)
	return val, errWithMsg("kHash getSet error", err, k.set(txn, KV{Key: key, Value: value}))
}

func (k *kHash) popRandom(txn *leveldb.Transaction, n int, fn func(key, value []byte) error) error {
	return k.scanRandom(txn, n, func(k1, v1 []byte) error {
		return errWithMsg("kHash popRandom error", fn(k1, v1), k.del(txn, k1))
	})
}

func (k *kHash) pop(txn *leveldb.Transaction, fn func(key, value []byte) error) error {
	return k.db.scanWithPrefix(txn, true, k.getPrefix(), func(key, value []byte) error {
		return errWithMsg("kHash pop error", fn(key, value), k.del(txn, key))
	})
}

func (k *kHash) popN(txn *leveldb.Transaction, n int, fn func(key, value []byte) error) error {
	return errWithMsg("kHash popn error", k.pop(txn, func(key, value []byte) error {
		if n < 1 {
			return io.EOF
		}
		n--
		return fn(key, value)
	}))
}

// ScanRandom 根据前缀随机扫描
func (k *kHash) scanRandom(txn *leveldb.Transaction, count int, fn func(key, value []byte) error) error {
	errmsg := "kHash scanRandom error"
	l, err := k.len()
	if l < count {
		return errWithMsg(errmsg, err, k.db.scanWithPrefix(txn, true, k.getPrefix(), fn))
	}

	m := -1
	rmd := genRandom(0, l, count)
	return errWithMsg(errmsg, k.db.scanWithPrefix(txn, true, k.getPrefix(), func(key, value []byte) error {
		m++
		if rmd[m] {
			err = fn(key, value)
		}
		return err
	}))
}

func (k *kHash) len() (int, error) {
	errMsg := "kHash len error"
	l, err := k.db.sizeof(k.getPrefix())
	return l, errWithMsg(errMsg, err)
}
