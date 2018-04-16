package kdb

import "github.com/dgraph-io/badger"

type KLBatch struct {
	kl  *KList
	txn *badger.Txn
}

func (k *KLBatch) Push(vs ... []byte) error {
	l := len(vs)

	i := k.kl.count
	for _, v := range vs {
		if err := k.txn.Set(k.kl.K(i), v); err != nil {
			return err
		}
		i++
	}

	if err := k.INCRBY(l); err != nil {
		return err
	}

	k.kl.count += l
	return nil
}

func (k *KLBatch) Pop() error {

}

func (k *KLBatch) INCRBY(n int) error {
	return k.kl.db.INCRBY(k.txn, k.kl.countKey, n)
}
