package kdb

import (
	"sync"
)

var (
	once sync.Once
	kdb  *KDB
)

func NewKV(k, v []byte) *KV {
	return &KV{Key: k, Value: v, Error: nil}
}

func ErrKV(err error) *KV {
	return &KV{Error: err}
}

type KV struct {
	Key   []byte
	Value []byte
	Error error
}

func (kv *KV) IsErr() bool {
	if kv.Error != nil {
		return true
	}
	return false
}
