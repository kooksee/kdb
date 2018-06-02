package kdb

import (
	"github.com/dgraph-io/badger"
	"github.com/kooksee/kdb/consts"
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

	return kh
}

// Prefix 前缀
func (h *KHash) Prefix() []byte {
	if len(h.prefix) == 0 {
		h.prefix = []byte(F("%s%s%s%s", consts.KHASH, consts.Separator, h.name, consts.Separator))
	}
	return h.prefix
}

func (h *KHash) FirstKey() []byte {
	if len(h.firstKey) == 0 {
		h.firstKey = h.Prefix()
	}
	return h.firstKey
}

func (h *KHash) LastKey() []byte {
	if len(h.lastKey) == 0 {
		h.lastKey = append(h.Prefix(), consts.MAXBYTE)
	}
	return h.lastKey
}

func (h *KHash) K(key []byte) []byte {
	return append(h.Prefix(), key...)
}

func (h *KHash) Get(key []byte) (val []byte, err error) {
	return val, h.db.GetWithTx(func(txn *badger.Txn) error {
		val, err = h.db.get(txn, key)
		return err
	})
}

func (h *KHash) Del(keys ... []byte) error {
	return h.db.UpdateWithTx(func(txn *badger.Txn) error {
		return (&KHBatch{txn: txn, kh: h}).MDel(keys...)
	})
}

func (h *KHash) Exist(k []byte) (b bool, err error) {
	return b, h.db.GetWithTx(func(txn *badger.Txn) error {
		b, err = (&KHBatch{txn: txn, kh: h}).Exist(k)
		return err
	})
}

func (h *KHash) Drop() error {
	return h.db.UpdateWithTx(func(txn *badger.Txn) error {
		return h.db.Drop(txn, h.Prefix())
	})
}

func (h *KHash) Len() (l int, err error) {
	return l, h.db.GetWithTx(func(txn *badger.Txn) error {
		l, err = h.db.Len(txn, h.Prefix())
		return err
	})
}

func (h *KHash) Set(key, value []byte) error {
	return h.MSet(&KV{key, value})
}

func (h *KHash) MSet(kvs ... *KV) error {
	return h.db.UpdateWithTx(func(txn *badger.Txn) error {
		return (&KHBatch{kh: h, txn: txn}).MSet(kvs...)
	})
}

func (h *KHash) PopRandom(n int, fn func(i int, key, value []byte) bool) error {
	return h.db.UpdateWithTx(func(txn *badger.Txn) error {
		return h.db.PopRandom(txn, h.Prefix(), n, fn)
	})
}

func (h *KHash) BatchView(fn func(*KHBatch) error) error {
	return h.db.GetWithTx(func(txn *badger.Txn) error {
		return fn(&KHBatch{kh: h, txn: txn})
	})
}

func (h *KHash) BatchUpdate(fn func(k *KHBatch) error) error {
	return h.db.UpdateWithTx(func(txn *badger.Txn) error {
		return fn(&KHBatch{kh: h, txn: txn})
	})
}
