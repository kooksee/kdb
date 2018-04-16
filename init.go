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
}

