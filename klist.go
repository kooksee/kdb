package kdb

import "github.com/boltdb/bolt"

type KList struct {
	db *kdb
}

func NewKList(db *bolt.DB, name string) (*KList, error) {
	_kdb, err := newKdb(db, name, "list")
	if err != nil {
		return nil, err
	}

	return &KList{_kdb}, nil
}

func (this *KList)Push(value ...[]byte) error {
	return this.db.push(value...)
}

func (this *KList)Set(index int, value []byte) error {
	return this.db.set(IntToByte(index), value)
}

func (this *KList)Get(index int) error {
	return this.db.get(IntToByte(index))
}

func (this *KList)Length() error {
	return this.db.length()
}

func (this *KList)First() error {
	return this.db.first()
}

func (this *KList)Last() error {
	return this.db.last()
}



