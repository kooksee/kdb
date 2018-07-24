package kdb

import (
	"testing"
	"fmt"
)

func TestKhash(t *testing.T) {
	InitKdb()

	db := GetKdb()

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
