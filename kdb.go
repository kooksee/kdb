// Package simplebolt provides a simple way to use the Bolt database.
// The API design is similar to xyproto/simpleredis, and the database backends
// are interchangeable, by using the xyproto/pinterface package.
package kdb

import (
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

const (
	// Version number. Stable API within major version numbers.
	Version = 3.0
)

type (
	// A Bolt database
	Database bolt.DB

	// Used for each of the datatypes
	boltBucket struct {
		db   *Database // the Bolt database
		name []byte    // the bucket name
	}

	listBucket struct {
		db     *Database // the Bolt database
		name   []byte    // the bucket name
		unique bool
	}

	// The wrapped datatypes
	List     listBucket
	Set      boltBucket
	HashMap  boltBucket
	KeyValue boltBucket
)

var (
	ErrBucketNotFound = errors.New("Bucket not found")
	ErrKeyNotFound = errors.New("Key not found")
	ErrDoesNotExist = errors.New("Does not exist")
	ErrFoundIt = errors.New("Found it")
	ErrExistsInSet = errors.New("Element already exists in set")
	ErrInvalidID = errors.New("Element ID can not contain \":\"")
	ErrEmpty = errors.New("empty data")
)

/* --- Database functions --- */

// Create a new bolt database
func New(filename string) (*Database, error) {
	// Use a timeout, in case the database file is already in use
	db, err := bolt.Open(filename, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	// store metadata
	if err = (*bolt.DB)(db).Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists("__metadata")
		return err
	}); err != nil {
		return nil, errors.New("Could not create bucket: " + err.Error())
	}

	return (*Database)(db), nil
}
// Close the database
func (db *Database) Close() {
	(*bolt.DB)(db).Close()
}

// Ping the database (only for fulfilling the pinterface.IHost interface)
func (db *Database) Ping() error {
	// Always O.K.
	return nil
}

/* --- List functions --- */

// Create a new list
func NewList(db *Database, id string) (*List, error) {
	name := []byte(id)
	if err := (*bolt.DB)(db).Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return errors.New("Could not create bucket: " + err.Error())
		}
		return nil // Return from Update function
	}); err != nil {
		return nil, err
	}
	// Success
	return &List{db, name}, nil
}

// Add an element to the list
func (l *List) Push(value ...string) error {
	if l.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}

		for _, v := range value {
			n, err := bucket.NextSequence()
			if err != nil {
				return err
			}

			err = bucket.Put(byteID(n), []byte(v))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Remove this list
func (l *List) Pop() (result string, err error) {
	if l.name == nil {
		return "", ErrDoesNotExist
	}
	return result, (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		result = ""

		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}

		cursor := bucket.Cursor()
		key, value := cursor.First()
		if key == nil {
			bucket.SetSequence(0)
			return ErrEmpty
		}

		bucket.Delete(key)
		result = string(value)

		return nil
	})
}

// Remove this list
func (l *List) PopN(n int) (result []string, err error) {
	if l.name == nil {
		return []string{}, ErrDoesNotExist
	}
	return result, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		result = []string{}

		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}

		for n > 0 {
			cursor := bucket.Cursor()
			key, value := cursor.First()
			if key == nil {
				bucket.SetSequence(0)
				return ErrEmpty
			}

			bucket.Delete(key)
			result = append(result, string(value))
			n--
		}
		return nil
	})
}


// Remove this list
func (l *List) Remove() error {
	err := (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(l.name))
	})
	// Mark as removed by setting the name to nil
	l.name = nil
	return err
}

func (l *List) Set(n int, value string) error {
	if l.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(l.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}

		return bucket.Put(byteID(n), []byte(value))
	})
}

func (l *List) Get(n int) (result string, err error) {
	if l.name == nil {
		return "", ErrDoesNotExist
	}

	return result, (*bolt.DB)(l.db).View(func(tx *bolt.Tx) error {
		result = ""

		bucket := tx.Bucket(l.name)
		if bucket == nil {
			return ErrBucketNotFound
		}

		result = bucket.Get(byteID(n))
		if result == nil {
			return ErrKeyNotFound
		}
		return nil
	})
}

// Get all elements of a list
func (l *List) GetAll() (results []string, err error) {
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

// Get the last element of a list
func (l *List) GetLast() (result string, err error) {
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

// Get the last N elements of a list
func (l *List) GetLastN(n int) (results []string, err error) {
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
			return errors.New("Too few items in list")
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



// Remove all elements from this list
func (l *List) Clear() error {
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

// Create a new HashMap
func NewHashMap(db *Database, id string) (*HashMap, error) {
	name := []byte(id)
	if err := (*bolt.DB)(db).Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return errors.New("Could not create bucket: " + err.Error())
		}
		return nil // Return from Update function
	}); err != nil {
		return nil, err
	}
	// Success
	return &HashMap{db, name}, nil
}

// Set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (h *HashMap) Set(elementid, key, value string) (err error) {
	if h.name == nil {
		return ErrDoesNotExist
	}
	if strings.Contains(elementid, ":") {
		return ErrInvalidID
	}
	return (*bolt.DB)(h.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(h.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		// Store the key and value
		return bucket.Put([]byte(elementid + ":" + key), []byte(value))
	})
}

// Get all elementid's for all hash elements
func (h *HashMap) GetAll() (results []string, err error) {
	if h.name == nil {
		return nil, ErrDoesNotExist
	}
	return results, (*bolt.DB)(h.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(h.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(byteKey, _ []byte) error {
			combinedKey := string(byteKey)
			if strings.Contains(combinedKey, ":") {
				fields := strings.SplitN(combinedKey, ":", 2)
				for _, result := range results {
					if result == fields[0] {
						// Result already exists, continue
						return nil // Continue ForEach
					}
				}
				// Store the new result
				results = append(results, string(fields[0]))
			}
			return nil // Continue ForEach
		})
	})
}

// Get a value from a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (h *HashMap) Get(elementid, key string) (val string, err error) {
	if h.name == nil {
		return "", ErrDoesNotExist
	}
	err = (*bolt.DB)(h.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(h.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		byteval := bucket.Get([]byte(elementid + ":" + key))
		if byteval == nil {
			return ErrKeyNotFound
		}
		val = string(byteval)
		return nil // Return from View function
	})
	return
}

// Check if a given elementid + key is in the hash map
func (h *HashMap) Has(elementid, key string) (found bool, err error) {
	if h.name == nil {
		return false, ErrDoesNotExist
	}
	return found, (*bolt.DB)(h.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(h.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		byteval := bucket.Get([]byte(elementid + ":" + key))
		if byteval != nil {
			found = true
		}
		return nil // Return from View function
	})
}

// Check if a given elementid exists as a hash map at all
func (h *HashMap) Exists(elementid string) (found bool, err error) {
	if h.name == nil {
		return false, ErrDoesNotExist
	}
	return found, (*bolt.DB)(h.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(h.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		bucket.ForEach(func(byteKey, byteValue []byte) error {
			combinedKey := string(byteKey)
			if strings.Contains(combinedKey, ":") {
				fields := strings.SplitN(combinedKey, ":", 2)
				if fields[0] == elementid {
					found = true
					return ErrFoundIt
				}
			}
			return nil // Continue ForEach
		})
		return nil // Return from View function
	})
}

// Remove a key for an entry in a hashmap (for instance the email field for a user)
func (h *HashMap) DelKey(elementid, key string) error {
	if h.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(h.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(h.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Delete([]byte(elementid + ":" + key))
	})
}

// Remove an element (for instance a user)
func (h *HashMap) Del(elementid string) error {
	if h.name == nil {
		return ErrDoesNotExist
	}
	// Remove the keys starting with elementid + ":"
	return (*bolt.DB)(h.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(h.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(byteKey, byteValue []byte) error {
			combinedKey := string(byteKey)
			if strings.Contains(combinedKey, ":") {
				fields := strings.SplitN(combinedKey, ":", 2)
				if fields[0] == elementid {
					return bucket.Delete([]byte(combinedKey))
				}
			}
			return nil // Continue ForEach
		})
	})
}

// Remove this hashmap
func (h *HashMap) Remove() error {
	err := (*bolt.DB)(h.db).Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(h.name))
	})
	// Mark as removed by setting the name to nil
	h.name = nil
	return err
}

// Remove all elements from this hash map
func (h *HashMap) Clear() error {
	if h.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(h.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(h.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(key, _ []byte) error {
			return bucket.Delete(key)
		})
	})
}

/* --- KeyValue functions --- */

// Create a new key/value if it does not already exist
func NewKeyValue(db *Database, id string) (*KeyValue, error) {
	name := []byte(id)
	if err := (*bolt.DB)(db).Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return errors.New("Could not create bucket: " + err.Error())
		}
		return nil // Return from Update function
	}); err != nil {
		return nil, err
	}
	return &KeyValue{db, name}, nil
}

// Set a key and value
func (kv *KeyValue) Set(key, value string) error {
	if kv.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(kv.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kv.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Put([]byte(key), []byte(value))
	})
}

// Get a value given a key
// Returns an error if the key was not found
func (kv *KeyValue) Get(key string) (val string, err error) {
	if kv.name == nil {
		return "", ErrDoesNotExist
	}
	err = (*bolt.DB)(kv.db).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kv.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		byteval := bucket.Get([]byte(key))
		if byteval == nil {
			return ErrKeyNotFound
		}
		val = string(byteval)
		return nil // Return from View function
	})
	return
}

// Remove a key
func (kv *KeyValue) Del(key string) error {
	if kv.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(kv.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kv.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Delete([]byte(key))
	})
}

// Increase the value of a key, returns the new value
// Returns an empty string if there were errors,
// or "0" if the key does not already exist.
func (kv *KeyValue) Inc(key string) (val string, err error) {
	if kv.name == nil {
		kv.name = []byte(key)
	}
	return val, (*bolt.DB)(kv.db).Update(func(tx *bolt.Tx) error {
		// The numeric value
		num := 0
		// Get the string value
		bucket := tx.Bucket(kv.name)
		if bucket == nil {
			// Create the bucket if it does not already exist
			bucket, err = tx.CreateBucketIfNotExists(kv.name)
			if err != nil {
				return errors.New("Could not create bucket: " + err.Error())
			}
		} else {
			val := string(bucket.Get([]byte(key)))
			if converted, err := strconv.Atoi(val); err == nil {
				// Conversion successful
				num = converted
			}
		}
		// Num is now either 0 or the previous numeric value
		num++
		// Convert the new value to a string and save it
		val = strconv.Itoa(num)
		err = bucket.Put([]byte(key), []byte(val))
		return err
	})
}

// Remove this key/value
func (kv *KeyValue) Remove() error {
	err := (*bolt.DB)(kv.db).Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(kv.name))
	})
	// Mark as removed by setting the name to nil
	kv.name = nil
	return err
}

// Remove all elements from this key/value
func (kv *KeyValue) Clear() error {
	if kv.name == nil {
		return ErrDoesNotExist
	}
	return (*bolt.DB)(kv.db).Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(kv.name)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(key, _ []byte) error {
			return bucket.Delete(key)
		})
	})
}

/* --- Utility functions --- */

// Create a byte slice from an uint64
func byteID(x uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, x)
	return b
}
