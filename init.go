package kdb

import (
	"sync"
)

var (
	once sync.Once
	kdb  *KDB
)

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
