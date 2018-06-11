package kdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"unsafe"
	"strconv"
	"math/rand"
	"time"
	"strings"
)

func IsFileExist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func If(b bool, trueVal, falseVal interface{}) interface{} {
	if b {
		return trueVal
	}
	return falseVal
}

// 范围判断 min <= v <= max
func Between(v, min, max []byte) bool {
	return bytes.Compare(v, min) >= 0 && bytes.Compare(v, max) <= 0
}

// 复制数组
func CopyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// 使用二进制存储整形
func IntToByte(x int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(x))
	return b
}

func ByteToInt(x []byte) int {
	return int(binary.BigEndian.Uint64(x))
}

func F(format string, a ...interface{}) string {
	return fmt.Sprintf(format, a...)
}

func Errs(errs ... string) string {
	return strings.Join(errs, "\n")
}

// S2b converts string to a byte slice without memory allocation.
// "abc" -> []byte("abc")
func S2b(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

// B2s converts byte slice to a string without memory allocation.
// []byte("abc") -> "abc" s
func B2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// B2ds return a Digit string of v
// v (8-byte big endian) -> uint64(123456) -> "123456".
func B2ds(v []byte) string {
	return strconv.FormatUint(binary.BigEndian.Uint64(v), 10)
}

// Btoi return an int64 of v
// v (8-byte big endian) -> uint64(123456).
func B2i(v []byte) uint64 {
	return binary.BigEndian.Uint64(v)
}

// DS2i returns uint64 of Digit string
// v ("123456") -> uint64(123456).
func DS2i(v string) uint64 {
	i, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return uint64(0)
	}
	return i
}

// Itob returns an 8-byte big endian representation of v
// v uint64(123456) -> 8-byte big endian.
func I2b(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// DS2b returns an 8-byte big endian representation of Digit string
// v ("123456") -> uint64(123456) -> 8-byte big endian.
func DS2b(v string) []byte {
	i, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return []byte("")
	}
	return I2b(i)
}

// BConcat concat a list of byte
func BConcat(slices ... []byte) []byte {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	tmp := make([]byte, totalLen)
	var i int
	for _, s := range slices {
		i += copy(tmp[i:], s)
	}
	return tmp
}

func BMap(m [][]byte, fn func(i int, k []byte) []byte) [][]byte {
	for i, d := range m {
		m[i] = fn(i, d)
	}
	return m
}

func KVMap(m []*KV, fn func(int, *KV) *KV) []*KV {
	for i, d := range m {
		m[i] = fn(i, d)
	}
	return m
}

// 生成count个[start,end)结束的不重复的随机数
func GenRandom(start int, end int, count int) map[int]bool {
	nums := make(map[int]bool)

	// 范围检查
	if end < start || (end-start) < count {
		return nums
	}

	// 随机数生成器，加入时间戳保证每次生成的随机数不一样
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(nums) < count {

		// 生成随机数
		num := r.Intn(end-start) + start
		if nums[num] {
			continue
		}
		nums[num] = true
	}

	return nums
}
