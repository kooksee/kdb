package kdb

import "github.com/dgraph-io/badger"

type KList struct {
	name     string
	prefix   []byte
	firstKey []byte
	lastKey  []byte
	count    int

	db *KDB
}

func NewKList(name string, db *KDB) *KList {
	kl := &KList{name: name, db: db}

	// 设置前缀
	kl.prefix = kl.Prefix()
	kl.firstKey = kl.FirstKey()
	kl.lastKey = kl.LastKey()

	cnt, err := kl.Len()
	if err != nil {
		panic(err.Error())
	}
	kl.count = cnt

	return kl
}

func (h *KList) Prefix() []byte {
	return h.db.KLPrefix([]byte(h.name))
}

func (h *KList) FirstKey() []byte {
	return h.Prefix()
}

func (h *KList) LastKey() []byte {
	return append(h.Prefix(), MAXBYTE)
}

func (h *KList) K(key int) []byte {
	return h.db.K(h.prefix, IntToByte(key))
}

func (h *KList) Len() (l int, err error) {
	return l, h.db.GetWithTx(func(txn *badger.Txn) error {
		l, err = (&KLBatch{txn: txn, kl: h}).Len()
		return err
	})
}

func (h *KList) BatchView(fn func(*KLBatch) error) error {
	return h.db.GetWithTx(func(txn *badger.Txn) error {
		return fn(&KLBatch{kl: h, txn: txn})
	})
}

func (h *KList) BatchUpdate(fn func(k *KLBatch) error) error {
	return h.db.UpdateWithTx(func(txn *badger.Txn) error {
		return fn(&KLBatch{kl: h, txn: txn})
	})
}
