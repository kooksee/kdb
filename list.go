package kdb

import "github.com/dgraph-io/badger"

type KList struct {
	name     string
	prefix   []byte
	firstKey []byte
	lastKey  []byte
	countKey []byte
	count    int

	db *KDB
}

func NewKList(name string, db *KDB) *KList {
	kl := &KList{name: name, db: db}

	// 设置前缀
	kl.prefix = kl.Prefix()
	kl.countKey = kl.CountKey()
	kl.firstKey = kl.FirstKey()
	kl.lastKey = kl.LastKey()

	err := kl.db.UpdateWithTx(func(txn *badger.Txn) error {

		// 设置类型第一个值
		return kl.db.mSet(
			txn,
			&KV{kl.firstKey, []byte("ok")},
			&KV{kl.LastKey(), []byte("ok")},
		)
	})

	if err != nil {
		panic(err.Error())
	}

	return kl
}

func (h *KList) Prefix() []byte {
	return []byte(f("l/%s/", h.name))
}

func (h *KList) FirstKey() []byte {
	return h.prefix
}

func (h *KList) LastKey() []byte {
	return append(h.prefix, MAXBYTE)
}

func (h *KList) CountKey() []byte {
	return h.db.CountKey(h.prefix)
}

func (h *KList) K(key int) []byte {
	return BConcat(h.prefix, IntToByte(key))
}
