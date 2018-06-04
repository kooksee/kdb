package kdb

import "testing"

const dbpath = "kdata"

func t1(f func() bool) func(func()) {
	return func(f1 func()) {
		if f() {
			f1()
		}
	}
}

func TestKhash(t *testing.T) {
	InitKdb(dbpath)

	db := GetKdb()

	kh, err := db.KHash("hello")
	if err != nil {
		panic(err.Error())
	}
	t.Log(kh.Prefix())
	t1(func() bool {
		return true
	})(func() {
		
	})
}
