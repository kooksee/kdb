package kdb

import "encoding/binary"


// Create a byte slice from an uint64
func IntToByte(x uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, x)
	return b
}

func ByteToInt(x []byte) uint64 {
	return binary.BigEndian.Uint64(x)
}


//fields := strings.SplitN(combinedKey, ":", 2)
//strings.Contains(combinedKey, ":")
// converted, err := strconv.Atoi(val)
//val = strconv.Itoa(num)