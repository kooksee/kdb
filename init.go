package kdb

import (
	"sync"
	"github.com/json-iterator/go"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
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
