package kdb

import (
	"testing"
	"fmt"
)

const dbpath = "kdata"

func TestKhash(t *testing.T) {
	InitKdb(dbpath)
	InitLog(nil)

	db := GetKdb()

	kh := db.KHash("hello")

	kh.Set([]byte("dd"), []byte("sddd"))
	kh.Set([]byte("dd1"), []byte("sddd1"))
	kh.Set([]byte("dd2"), []byte("sddd2"))

	kh.BatchView(func(batch *KHBatch) error {
		return batch.Range(func(key, value []byte) error {
			fmt.Println(string(key), string(value))
			return nil
		})
	})

	fmt.Println(kh.Len())
	fmt.Println(string(kh.Get([]byte("dd"))))
	fmt.Println(string(kh.Get([]byte("dd1"))))
	fmt.Println(string(kh.Get([]byte("dd2"))))
}
