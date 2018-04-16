package kdb

import (
	"github.com/dgraph-io/badger"
	"bytes"
	"github.com/kataras/iris/core/errors"
)

type KLBatch struct {
	kl  *KList
	txn *badger.Txn
}

func (k *KLBatch) Push(vs ... []byte) error {

	i := k.kl.count
	for _, v := range vs {
		if err := k.txn.Set(k.kl.K(i), v); err != nil {
			return err
		}
		i++
	}

	k.kl.count = i
	return nil
}

func (k *KLBatch) RPop() (v []byte, err error) {
	return v, k.RPopNIter(1, func(i int, key, value []byte) {
		v = value
	})
}

func (k *KLBatch) LPop() (v []byte, err error) {
	return v, k.LPopNIter(1, func(i int, key, value []byte) {
		v = value
	})
}

func (k *KLBatch) RPopNIter(n int, fn func(i int, key, value []byte)) error {

	i := k.kl.count
	if err := k.kl.db.PopN(k.txn, true, k.kl.firstKey, k.kl.lastKey, n, func(i int, key, value []byte) bool {
		fn(i, key, value)
		i--
		return true
	}); err != nil {
		return err
	}

	k.kl.count = i
	return nil
}

func (k *KLBatch) RPopN(n int) (vals [][]byte, err error) {

	i := k.kl.count
	if err := k.kl.db.PopN(k.txn, true, k.kl.firstKey, k.kl.lastKey, n, func(i int, key, value []byte) bool {
		vals[i] = value
		i--
		return true
	}); err != nil {
		return nil, err
	}

	k.kl.count = i
	return vals, nil
}

func (k *KLBatch) LPopN(n int) (vals [][]byte, err error) {

	i := k.kl.count
	if err := k.kl.db.PopN(k.txn, false, k.kl.firstKey, k.kl.lastKey, n, func(i int, key, value []byte) bool {
		vals[i] = value
		i--
		return true
	}); err != nil {
		return nil, err
	}

	k.kl.count = i
	return vals, nil
}

func (k *KLBatch) LPopNIter(n int, fn func(i int, key, value []byte)) error {

	i := k.kl.count
	if err := k.kl.db.PopN(k.txn, false, k.kl.firstKey, k.kl.lastKey, n, func(i int, key, value []byte) bool {
		fn(i, key, value)
		i--
		return true
	}); err != nil {
		return err
	}

	k.kl.count = i
	return nil
}

func (k *KLBatch) Len() (int, error) {
	return k.kl.db.Len(k.txn, k.kl.prefix)
}

func (k *KLBatch) Index(i int) ([]byte, error) {
	v, err := k.kl.db.get(k.txn, k.kl.K(i))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (k *KLBatch) Range(start, end int) (vals [][]byte, err error) {
	return vals, k.kl.db.ScanRange(k.txn, false, k.kl.K(start), k.kl.K(end), func(i int, key, value []byte) bool {
		vals[i] = value
		return true
	})
}

func (k *KLBatch) RangeIter(start, end int, fn func(i int, key int, value []byte)) error {
	return k.kl.db.ScanRange(k.txn, false, k.kl.K(start), k.kl.K(end), func(i int, key, value []byte) bool {
		fn(i, ByteToInt(bytes.TrimPrefix(key, k.kl.prefix)), value)
		return true
	})
}

func (k *KLBatch) Rem(start, end int) error {
	if start > end {
		return errors.New("start must be less than end")
	}

	if end > k.kl.count {
		end = k.kl.count
	}

	var vals [][]byte
	for i := start; i <= end; i++ {
		vals = append(vals, k.kl.K(i))
	}
	if err := k.kl.db.mDel(k.txn, vals...); err != nil {
		return err
	}

}
