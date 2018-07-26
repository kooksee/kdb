package kdb

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type kHash struct {
	IKHash

	name []byte

	prefix   []byte
	firstKey []byte
	lastKey  []byte

	db IKDB
}

// newkHash 初始化kHash
func newkHash(name []byte, db *kDb) *kHash {
	kh := &kHash{name: name, db: db}
	if px, err := db.recordPrefix(name); err != nil {
		mustNotErr(errWithMsg("NewkHash recordPrefix", err))
	} else {
		kh.prefix = px
	}

	return kh
}

// Prefix 前缀
func (k *kHash) getPrefix() []byte {
	return k.prefix
}

func (k *kHash) k(key []byte) []byte {
	return append(k.prefix, key...)
}

func (k *kHash) Get(key []byte) ([]byte, error) {
	return k.get(nil, key)
}

func (k *kHash) Set(key, value []byte) error {
	return k.set(nil, KV{Key: key, Value: value})
}

func (k *kHash) MSet(kv ... KV) error {
	return k.set(nil, kv...)
}

func (k *kHash) Del(key ... []byte) error {
	return k.del(nil, key...)
}

func (k *kHash) Exist(key []byte) (bool, error) {
	return k.exist(nil, key)
}

func (k *kHash) Drop() error {
	return k.db.WithTxn(func(tx *leveldb.Transaction) error {
		if err := k.db.scanWithPrefix(tx, false, k.getPrefix(), func(key, value []byte) error {
			return k.del(tx, key)
		}); err != nil {
			return err
		}

		return k.db.saveBk(tx, k.name, k.prefix)
	})
}

func (k *kHash) Len() (int, error) {
	return k.len()
}

func (k *kHash) PopRandom(n int, fn func(key, value []byte) error) error {
	return k.popRandom(nil, n, fn)
}

func (k *kHash) Pop(fn func(key, value []byte) error) error {
	return k.pop(nil, fn)
}

func (k *kHash) PopN(n int, fn func(key, value []byte) error) error {
	return k.popN(nil, n, fn)
}

func (k *kHash) Range(fn func(key, value []byte) error) error {
	return k.db.scanWithPrefix(nil, false, k.getPrefix(), fn)
}

func (k *kHash) Reverse(fn func(key, value []byte) error) error {
	return k.db.scanWithPrefix(nil, true, k.getPrefix(), fn)
}

func (k *kHash) Map(fn func(key, value []byte) ([]byte, error)) error {
	return k._map(nil, fn)
}

// Union 合并
func (k *kHash) Union(otherNames ... []byte) error {
	return k.union(nil, otherNames...)
}

func (k *kHash) WithTx(fn func(kh IKHBatch) error) error {
	return k.db.WithTxn(func(tx *leveldb.Transaction) error {
		return fn(&kHBatch{kh: k, txn: tx})
	})
}
