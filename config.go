package kdb

import (
	"path/filepath"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
)

var cfg *config
var once sync.Once

type config struct {
	db IKDB
}

// InitKdb 初始化数据库
func (c *config) InitKdb(paths ... string) {
	path := filepath.Join("kdata", "db")
	if len(paths) > 0 {
		path = paths[0]
	}

	mustNotErr(ensureDir(path, 0755))

	db, err := leveldb.OpenFile(path, nil)
	mustNotErr(errWithMsg("the db start fail", err))

	c.db = &kDb{db: db, hmap: make(map[string]IKHash)}
}

// getDb 得到kDb实例
func (c *config) getDb() IKDB {
	if c.db == nil {
		panic("please init kdb")
	}
	return c.db
}

// GetDb 得到kdb实例
func (c *config) GetDb() IKDB {
	return c.getDb()
}

func DefaultConfig() *config {
	once.Do(func() {
		cfg = &config{}
	})
	return cfg
}
