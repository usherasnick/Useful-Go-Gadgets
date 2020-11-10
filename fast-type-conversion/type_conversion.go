package fasttypeconversion

import (
	"reflect"
	"unsafe"
)

// String2Bytes 字符串快速转换成字节数组, 新变量共享底层数据指针.
func String2Bytes(s string) []byte {
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: stringHeader.Data,
		Len:  stringHeader.Len,
		Cap:  stringHeader.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

// Bytes2String 字节数组快速转换成字符串, 新变量共享底层数据指针.
func Bytes2String(b []byte) string {
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{
		Data: sliceHeader.Data,
		Len:  sliceHeader.Len,
	}
	return *(*string)(unsafe.Pointer(&sh))
}
