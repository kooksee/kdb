# kdb

KDB是基于leveldb封装的简便数据库
KDB会提供类似于redis接口的操作

## example

```
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
```