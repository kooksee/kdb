package kdb

import (
	"testing"
)

const dbpath = "kdata"

func TestKhash(t *testing.T) {
	InitKdb(dbpath)

	db := GetKdb()

	kh := db.KHash("hello")
	kh.Set([]byte("dd"),[]byte("sddd"))
}
