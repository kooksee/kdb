package kdb

import "github.com/dgraph-io/badger"

type KHash struct {
	name     string
	prefix   []byte
	firstKey []byte
	lastKey  []byte
	countKey []byte
	count    int

	db *KDB
}

func NewKHash(name string, db *KDB) *KHash {
	kh := &KHash{name: name, db: db}

	// 设置前缀
	kh.prefix = kh.Prefix()
	kh.countKey = kh.CountKey()
	kh.firstKey = kh.FirstKey()
	kh.lastKey = kh.LastKey()

	err := kh.db.UpdateWithTx(func(txn *badger.Txn) error {
		cnt, err := kh.db.count(txn, kh.CountKey())
		if err != nil {
			return err
		}

		kh.count = cnt

		// 设置类型第一个值
		return kh.db.mSet(
			txn,
			&KV{kh.firstKey, []byte("ok")},
			&KV{kh.LastKey(), []byte("ok")},
		)
	})

	if err != nil {
		panic(err.Error())
	}

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

func (h *KHash) CountKey() []byte {
	return h.db.CountKey(h.prefix)
}

func (h *KHash) K(key []byte) []byte {
	return append(h.prefix, key...)
}

func (h *KHash) Get(key []byte) (v []byte, err error) {
	vals, err := h.MGet(key)
	if err != nil {
		return nil, err
	}

	return vals[0], nil
}

func (h *KHash) MGet(keys ... []byte) (vals [][]byte, err error) {
	return vals, h.db.GetWithTx(func(txn *badger.Txn) error {
		vals, err = h.db.mGet(txn, BMap(keys, func(_ int, k []byte) []byte {
			return h.K(k)
		})...)
		return err
	})
}

func (h *KHash) Del(k []byte) error {
	return h.MDel(k)
}

func (h *KHash) MDel(k ... []byte) error {
	return h.db.UpdateWithTx(func(txn *badger.Txn) error {
		return h.db.mDel(txn, k...)
	})
}

func (h *KHash) Exist(k []byte) (bool, error) {
	return h.db.exist(h.K(k))
}

func (h *KHash) Drop() error {
	return h.db.UpdateWithTx(func(txn *badger.Txn) error {
		return h.db.Drop(txn, h.prefix)
	})
}

func (h *KHash) Len() (l int, err error) {
	return l, h.db.GetWithTx(func(txn *badger.Txn) error {
		l, err = h.db.count(txn, h.countKey)
		return err
	})
}

func (h *KHash) Set(key, value []byte) error {
	return h.MSet(&KV{key, value})
}

func (h *KHash) MSet(kvs ... *KV) error {
	return h.db.UpdateWithTx(func(txn *badger.Txn) error {
		kb := &KBatch{kh: h, txn: txn}
		return kb.MSet(kvs...)
	})
}

func (h *KHash) BatchView(fn func(*KBatch) error) error {
	return h.db.GetWithTx(func(txn *badger.Txn) error {
		return fn(&KBatch{kh: h, txn: txn})
	})
}

func (h *KHash) BatchUpdate(fn func(k *KBatch) error) error {
	return h.db.UpdateWithTx(func(txn *badger.Txn) error {
		return fn(&KBatch{kh: h, txn: txn})
	})
}
