package kdb

import "github.com/boltdb/bolt"

type KList struct {
	db *kdb
}

type KValue struct {

}

func NewKList(db *bolt.DB, name string) (*KList, error) {
	_kdb, err := newKdb(db, name)
	if err != nil {
		return nil, err
	}

	return &KList{_kdb}, nil
}

func (this *KList)Push(value ...[]byte) error {
	return this.db.push(value...)
}

func (this *KList)RPop(n int) ([][]byte, error) {
	_, values, err := this.db.popN(n, 1)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (this *KList)LPop(n int) ([][]byte, error) {
	_, values, err := this.db.popN(n, 0)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (this *KList)PopRandom(n int) ([][]byte, error) {
	_, values, err := this.db.popByRandom(n)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (this *KList)Set(index int, value []byte) error {
	return this.db.set(IntToByte(index), value)
}

func (this *KList)Get(index int) []byte {
	return this.db.get(IntToByte(index))
}

func (this *KList)Length() int {
	return this.db.length()
}

func (this *KList)FirstN(n int) ([]byte, error) {
	_, values, err := this.db.first(n)
	if err != nil {
		return nil, err
	}
	return values, nil

}

func (this *KList)LastN(n int) error {
	_, values, err := this.db.last(n)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (this *KList)Range(start, end, skip int) error {
	return this.db.ranger(start, end, skip)
}

func (this *KList)Drop() error {
	return this.db.drop()
}

