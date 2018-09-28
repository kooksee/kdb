package kdb

import (
	"path/filepath"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
)

var cfg *config
var once sync.Once

type config struct {
	db     *kDb
	DbPath string
}

// InitKdb 初始化数据库
func (c *config) InitKdb() {

	mustNotErr("kdb.config.InitKdb", ensureDir(c.DbPath, 0755))

	db, err := leveldb.OpenFile(c.DbPath, nil)
	mustNotErr("kdb.config.InitKdb,the db start fail", err)

	c.db = &kDb{db: db, hmap: make(map[string]*kHash)}
	c.db.khashSizeLoad()
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
		cfg = &config{
			DbPath: filepath.Join("kdata", "db"),
		}
	})
	return cfg
}
