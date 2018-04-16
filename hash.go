package kdb

import "github.com/dgraph-io/badger"

type KHash struct {
	name     string
	prefix   []byte
	firstKey []byte
	lastKey  []byte

	db *KDB
}

// NewKHash 初始化khash
func NewKHash(name string, db *KDB) *KHash {
	kh := &KHash{name: name, db: db}

	// 设置前缀
	kh.prefix = kh.Prefix()
	kh.firstKey = kh.FirstKey()
	kh.lastKey = kh.LastKey()

	return kh
}

// Prefix 前缀
func (h *KHash) Prefix() []byte {
	return []byte(f("h/%s/", h.name))
}

func (h *KHash) FirstKey() []byte {
	return h.prefix
}

func (h *KHash) LastKey() []byte {
	return append(h.prefix, MAXBYTE)
}

func (h *KHash) K(key []byte) []byte {
	return BConcat(h.prefix, key)
}

func (h *KHash) Get(key []byte) ([]byte, error) {
	vals, err := h.MGet(key)
	return vals[0], err
}

func (h *KHash) MGet(keys ... []byte) (vals [][]byte, err error) {
	return vals, h.db.GetWithTx(func(txn *badger.Txn) error {
		vals, err = (&KHBatch{txn: txn, kh: h}).MGet(keys...)
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
		return h.db.Drop(txn, h.prefix)
	})
}

func (h *KHash) Len() (l int, err error) {
	return l, h.db.GetWithTx(func(txn *badger.Txn) error {
		l, err = (&KHBatch{txn: txn, kh: h}).Len()
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
