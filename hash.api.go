package kdb

import (
	"github.com/dgraph-io/badger"
	"time"
)

func (h *KHash) get(txn *badger.Txn, key []byte) (val []byte, err error) {
	return h.db.get(txn, h.K(key))
}

func (h *KHash) hDel(txn *badger.Txn, keys ... []byte) error {
	for _, key := range keys {
		if err := h.db.mDel(txn, h.K(key)); err != nil {
			return err
		}
	}
	return nil
}

func (h *KHash) exists(txn *badger.Txn, k []byte) (b bool, err error) {
	return h.exists(txn, h.K(k))
}

func (h *KHash) set(txn *badger.Txn, key, value []byte) error {
	return h.db.set(txn, h.K(key), value)
}

func (h *KHash) setWithTTL(txn *badger.Txn, key, val []byte, dur time.Duration) error {
	return txn.SetWithTTL(key, val, dur)
}

func (h *KHash) mSet(txn *badger.Txn, kvs ... *KV) error {
	return h.db.mSet(txn, KVMap(kvs, func(_ int, kv *KV) *KV {
		kv.Key = h.K(kv.Key)
		return kv
	})...)
}

func (h *KHash) union(txn *badger.Txn, otherName string) error {
	h1, err := h.db.KHash(otherName)
	if err != nil {
		return err
	}
	return h.db.Pop(txn, h.Prefix(), func(key, value []byte) error {
		return h1.set(txn, key, value)
	})
}
