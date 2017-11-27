//对kdb进行简单的封装

package kdb

import (
	"encoding/binary"
	"errors"
	"math/rand"

	"github.com/boltdb/bolt"
)

const (
	// 版本号
	Version = 3.0
)

type (
	// A Bolt database
	Database bolt.DB

	kdb struct {
		db   *Database // the Bolt database
		name []byte    // the bucket name
		mode string    // 设置模式,队列,字典,列表,集合等
	}

	// The wrapped datatypes
	KSet      kdb
	KHashMap  kdb
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

func newKdb(db *Database, id, mode string) (kdb, error) {

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

	return kdb{db:db, name:name, mode:mode}, nil
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
func (l *kdb) popN(n int, m int) (keys [][]byte, values [][]byte, err error) {
	return keys, values, (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		var (
			key []byte
			value []byte
		)

		bucket := tx.Bucket(l.name)
		length := ByteToInt(bucket.Get(konst.length))

		for n > 0 {
			cursor := bucket.Cursor()
			if m == 0 {
				key, value = cursor.First()
			} else {
				key, value = cursor.Last()
			}

			if key == nil {
				bucket.SetSequence(0)
				return ErrEmpty
			}

			bucket.Delete(key)
			keys = append(keys, key)
			values = append(values, value)
			n--
			length--
		}

		return bucket.Put(konst.length, IntToByte(length))
	})
}

func (l *kdb) popByRandom(n int) (keys [][]byte, values [][]byte, err error) {
	return keys, values, (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		random := map[int]bool{}

		bucket := tx.Bucket(l.name)
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

func (l *kdb) popByKeys(keys ...[]byte) (values [][]byte, err error) {
	return values, (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		var (
			value []byte
		)

		bucket := tx.Bucket(l.name)
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

func (l *kdb) drop() error {
	err := (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(l.name))
	})
	l.name = nil
	return err
}

func (l *kdb) keys() (ks [][]byte, err error) {
	return ks, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
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

func (l *kdb) set(k []byte, v []byte) error {
	return (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		if k == nil || v == nil {
			return errors.New("key or value 为空")
		}
		return bucket.Put(k, v)
	})
}

func (l *kdb) get(k []byte) (v []byte, err error) {
	return v, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		v = bucket.Get(k)
		if v == nil {
			return ErrKeyNotFound
		}
		return nil
	})
}

// Get all elements of a kdb
func (l *kdb) GetAll() (results []string, err error) {
	if l.name == nil {
		return nil, ErrDoesNotExist
	}
	return results, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(_, value []byte) error {
			results = append(results, string(value))
			return nil // Continue ForEach
		})
	})
}

// Get the last element of a kdb
func (l *kdb) GetLast() (result string, err error) {
	if l.name == nil {
		return "", ErrDoesNotExist
	}
	return result, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		cursor := bucket.Cursor()
		// Ignore the key
		_, value := cursor.Last()
		result = string(value)
		return nil // Return from View function
	})
}

// Get the last N elements of a kdb
func (l *kdb) GetLastN(n int) (results []string, err error) {
	if l.name == nil {
		return nil, ErrDoesNotExist
	}
	return results, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
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
func (l *kdb) Clear() error {
	if l.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(key, _ []byte) error {
			return bucket.Delete(key)
		})
	})
}

/* --- HashMap functions --- */


//fields := strings.SplitN(combinedKey, ":", 2)
//strings.Contains(combinedKey, ":")
// converted, err := strconv.Atoi(val)
//val = strconv.Itoa(num)

// Create a byte slice from an uint64
func IntToByte(x uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, x)
	return b
}

func ByteToInt(x []byte) uint64 {
	return binary.BigEndian.Uint64(x)
}