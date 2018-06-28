package kdb

import (
	"github.com/kooksee/kdb/consts"
	"github.com/syndtr/goleveldb/leveldb"
)

type KHash struct {
	name string

	prefix   []byte
	firstKey []byte
	lastKey  []byte

	db *KDB
}

// NewKHash 初始化khash
func NewKHash(name string, db *KDB) *KHash {
	kh := &KHash{name: name, db: db}
	if px, err := db.recordPrefix([]byte(name)); err != nil {
		panic(Errs("NewKHash recordPrefix", err.Error()))
	} else {
		kh.prefix = px
	}

	return kh
}

// Prefix 前缀
func (k *KHash) Prefix() []byte {
	return k.prefix
}

func (k *KHash) MinKey() []byte {
	if len(k.firstKey) == 0 {
		k.firstKey = k.Prefix()
	}
	return k.firstKey
}

func (k *KHash) MaxKey() []byte {
	if len(k.lastKey) == 0 {
		k.lastKey = append(k.Prefix(), consts.MAXBYTE)
	}
	return k.lastKey
}

func (k *KHash) K(key []byte) []byte {
	return append(k.Prefix(), key...)
}

func (k *KHash) Get(key []byte) ([]byte, error) {
	return k.get(nil, key)
}

func (k *KHash) Set(key,value []byte) error {
	return k.set(nil, KV{Key:key,Value:value})
}

func (k *KHash) MSet(kv ... KV) error {
	return k.set(nil, kv...)
}

func (k *KHash) Del(key ... []byte) error {
	return k.del(nil, key...)
}

func (k *KHash) Exist(key []byte) (bool, error) {
	return k.exist(nil, key)
}

func (k *KHash) Drop() error {
	return k.db.drop(nil, k.Prefix())
}

func (k *KHash) Len() (int, error) {
	return k.len()
}

func (k *KHash) PopRandom(n int, fn func(key, value []byte) error) error {
	return k.popRandom(nil, n, fn)
}

func (k *KHash) Pop(fn func(key, value []byte) error) error {
	return k.pop(nil, fn)
}

func (k *KHash) PopN(n int, fn func(key, value []byte) error) error {
	return k.popN(nil, n, fn)
}

func (k *KHash) Range(fn func(key, value []byte) error) error {
	return k.db.scanWithPrefix(nil, false, k.Prefix(), fn)
}

func (k *KHash) Reverse(fn func(key, value []byte) error) error {
	return k.db.scanWithPrefix(nil, true, k.Prefix(), fn)
}

func (k *KHash) Map(fn func(key, value []byte) ([]byte, error)) error {
	return k._map(nil, fn)
}

// Union 合并
func (k *KHash) Union(otherNames ... []byte) error {
	return k.union(nil, otherNames...)
}

func (k *KHash) WithTx(fn func(kh *KHBatch) error) error {
	return k.db.WithTxn(func(tx *leveldb.Transaction) error {
		return fn(&KHBatch{kh: k, txn: tx})
	})
}
