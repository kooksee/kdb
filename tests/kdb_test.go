package tests

import (
	"testing"
	"fmt"
	"github.com/kooksee/kdb"
)

func TestKhash(t *testing.T) {
	cfg := kdb.DefaultConfig()
	cfg.InitKdb()
	db := cfg.GetDb()

	k := db.KHash([]byte("hello"))
	k.Set([]byte("d"), []byte("f"))

	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("hello%d", i)
		fmt.Println(name)
		k1 := db.KHash([]byte(name))
		k1.Set([]byte("d"), []byte("f"))
	}

	db.ScanAll(func(key, value []byte) error {
		fmt.Println(string(key), string(value))
		return nil
	})

}
