package kdb

import (
	"github.com/json-iterator/go"
	"github.com/kooksee/cmn"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
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

func kvMap(m []KV, fn func(int, KV) KV) []KV {
	for i, d := range m {
		m[i] = fn(i, d)
	}
	return m
}

var errs = cmn.Err.Err
var errWithMsg = cmn.Err.ErrWithMsg
var mustNotErr = cmn.Err.MustNotErr
var genRandom = cmn.Rand.GenRandom
var bMap = cmn.BMap
var ensureDir = cmn.OS.EnsureDir
