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
		k1 := db.KHash([]byte(fmt.Sprintf("hello%d", i)))
		if err := k1.Set([]byte("d"), []byte("f")); err != nil {
			panic(err.Error())
		}
	}
	db.ScanAll(func(key, value []byte) {
		fmt.Println(string(key), string(value))
	})
}

func KHashNames() (names chan string, err error) {
	names1 := make(chan string, 1)
	names1 <- "hello"
	return names1, nil
}

func TestName1(t *testing.T) {
	a, _ := KHashNames()
	fmt.Println(<-a)
}
