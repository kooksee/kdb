//对kdb进行简单的封装

package kdb

import (
	"errors"
	"math/rand"

	"github.com/boltdb/bolt"
)

type (
	// A Bolt database
	Database bolt.DB

	kdb struct {
		db   *Database // the Bolt database
		name []byte    // the bucket name
	}
)

var (
	ErrBucketNotFound = errors.New("Bucket not found")
	ErrKeyNotFound = errors.New("Key not found")
	ErrDoesNotExist = errors.New("Does not exist")
	ErrFoundIt = errors.New("Found it")
	ErrExistsInSet = errors.New("Element already exists in set")
	ErrInvalidID = errors.New("Element ID can not contain \":\"")
	ErrEmpty = errors.New("empty data")

	konst = struct {
		firstIndex []byte
		lastIndex  []byte
		length     []byte
	}{
		firstIndex:[]byte("__firstIndex"),
		lastIndex :[]byte("__lastIndex"),
		length :[]byte("__length"),
	}
)




/* --- Database functions --- */

// db, err := bolt.Open(filename, 0600, &bolt.Options{Timeout: 1 * time.Second})
// Create a new bolt database

func (db *Database) Close() {
	(*bolt.DB)(db).Close()
}

func newKdb(db *Database, id string) (*kdb, error) {

	// 数据bucket的名字
	name := []byte(id)

	if err := (*bolt.DB)(db).Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(name)
		if err != nil {
			return errors.New("Could not create bucket: " + err.Error())
		}

		// 初始化元数据
		// 记录数据的长度
		bucket.Put(konst.length, IntToByte(0))

		// 记录最小序列号
		bucket.Put(konst.firstIndex, IntToByte(0))

		// 记录最大序列号
		bucket.Put(konst.lastIndex, IntToByte(0))

		return nil
	}); err != nil {
		return nil, err
	}

	return &kdb{db:db, name:name}, nil
}

/* --- kdb functions --- */

// 把数据放到队列的尾部
func (k *kdb) push(value ...[]byte) error {
	return (*bolt.DB)(k.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(k.name)
		length := ByteToInt(bucket.Get(konst.length))
		for _, v := range value {
			if err := bucket.Put(IntToByte(bucket.Sequence()), v); err != nil {
				return err
			}

			length ++
			bucket.NextSequence()
		}
		return bucket.Put(konst.length, IntToByte(length))
	})
}


// 取数据
func (k *kdb) popN(n int, m int) (keys [][]byte, values [][]byte, err error) {
	return keys, values, (*bolt.DB)(k.db).Update(func(tx *bolt.Tx) error {
		var (
			value []byte
		)

		bucket := tx.Bucket(k.name)
		length := ByteToInt(bucket.Get(konst.length))
		cursor := bucket.Cursor()
		if m == 0 {
			for key, _ := cursor.First(); n > 0; key, _ = cursor.Next() {
				if key == nil {
					bucket.SetSequence(0)
					length = 0
					break
				}
				bucket.Delete(key)
				keys = append(keys, key)
				values = append(values, value)
				n--
				length--
			}
		}

		if m == 1 {
			for key, _ := cursor.Last(); n > 0; key, _ = cursor.Prev() {
				if key == nil {
					bucket.SetSequence(0)
					length = 0
					break
				}
				bucket.Delete(key)
				keys = append(keys, key)
				values = append(values, value)
				n--
				length--
			}
		}
		return bucket.Put(konst.length, IntToByte(length))
	})
}

func (k *kdb) popByRandom(n int) (keys [][]byte, values [][]byte, err error) {
	return keys, values, (*bolt.DB)(k.db).Update(func(tx *bolt.Tx) error {
		random := map[int]bool{}

		bucket := tx.Bucket(k.name)
		length := ByteToInt(bucket.Get(konst.length))

		if n > length {
			return errors.New("长度大于数据长度")
		}

		// 获取随机数
		for n > 0 {
			for {
				n := rand.Intn(length)
				if !random[n] {
					random[n] = true
					break
				}
			}
			n--
		}

		_n := 0
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			if !random[_n] {
				_n++
				continue
			}

			keys = append(keys, key)
			values = append(values, value)
			bucket.Delete(key)
			delete(random, _n)
			length--

			if len(random) == 0 {
				break
			}

		}
		return bucket.Put(konst.length, IntToByte(length))
	})
}

func (k *kdb) popByKeys(keys ...[]byte) (values [][]byte, err error) {
	return values, (*bolt.DB)(k.db).Update(func(tx *bolt.Tx) error {
		var (
			value []byte
		)

		bucket := tx.Bucket(k.name)
		length := ByteToInt(bucket.Get(konst.length))

		if length <= 0 {
			return errors.New("没有数据")
		}

		for _, k := range keys {
			value = bucket.Get(k)
			if value == nil {
				return ErrEmpty
			}

			bucket.Delete(k)
			values = append(values, value)
			length--
		}

		return bucket.Put(konst.length, IntToByte(length))
	})
}

func (k *kdb) drop() error {
	err := (*bolt.DB)(k.db).Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(k.name))
	})
	k.name = nil
	return err
}

func (k *kdb) keys() (ks [][]byte, err error) {
	return ks, (*bolt.DB)(k.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(k.name)
		cursor := bucket.Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			ks = append(ks, key)
		}
		return nil
	})
}

func (l *kdb) values() (vs [][]byte, err error) {
	return vs, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		cursor := bucket.Cursor()
		for _, value := cursor.First(); value != nil; _, value = cursor.Next() {
			vs = append(vs, value)
		}
		return nil
	})
}

func (this *kdb) set(k []byte, v []byte) error {
	return (*bolt.DB)(this.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(this.name)
		if k == nil || v == nil {
			return errors.New("key or value 为空")
		}
		return bucket.Put(k, v)
	})
}

func (this *kdb) get(k []byte) (v []byte, err error) {
	return v, (*bolt.DB)(this.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(this.name)
		return bucket.Get(k)
	})
}

// Get all elements of a kdb
func (k *kdb) GetAll() (results []string, err error) {
	return results, (*bolt.DB)(k.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(k.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(_, value []byte) error {
			results = append(results, string(value))
			return nil // Continue ForEach
		})
	})
}

func (k *kdb)length() (n int) {
	return n, (*bolt.DB)(k.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(k.name)
		n = ByteToInt(bucket.Get(konst.length))
		return nil
	})
}

func (this *kdb)first(n int) (k, v [][]byte, err error) {
	return k, v, (*bolt.DB)(this.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(this.name)
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil && n > 0; key, value = cursor.Next() {
			k = append(k, key)
			v = append(v, value)
			n--
		}
		return nil
	})
}

func (this *kdb)last(n int) (k, v [][]byte, err error) {
	return v, (*bolt.DB)(this.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(this.name)
		cursor := bucket.Cursor()
		for key, value := cursor.Last(); key != nil && n > 0; key, value = cursor.Prev() {
			k = append(k, key)
			v = append(v, value)
			n--
		}
		return nil
	})
}

func (k *kdb)ranger(start, end, skip int) (v [][]byte, err error) {
	return v, (*bolt.DB)(k.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(k.name)
		_, v = bucket.Cursor().Last()
		return nil
	})
}

func (k *kdb)filterF(f func() bool) {
}

func (k *kdb)mapF(f func() []byte) {
}

func (k *kdb)reduceF(f func() []byte) {
}


// Get the last N elements of a kdb
func (k *kdb) GetLastN(n int) (results []string, err error) {
	return results, (*bolt.DB)(k.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(k.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		var size int64
		bucket.ForEach(func(_, _ []byte) error {
			size++
			return nil // Continue ForEach
		})
		if size < int64(n) {
			return errors.New("Too few items in kdb")
		}
		// Ok, fetch the n last items. startPos is counting from 0.
		var (
			startPos = size - int64(n)
			i int64
		)
		bucket.ForEach(func(_, value []byte) error {
			if i >= startPos {
				results = append(results, string(value))
			}
			i++
			return nil // Continue ForEach
		})
		return nil // Return from View function
	})
}



// Remove all elements from this kdb
func (k *kdb) Clear() error {
	return (*bolt.DB)(k.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(k.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(key, _ []byte) error {
			return bucket.Delete(key)
		})
	})
}

