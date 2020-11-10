package fasttypeconversion

import "testing"

const N = 1e8 // 10000w

/*
BenchmarkNormalString2Bytes-12    	2000000000	         0.26 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkNormalString2Bytes(b *testing.B) {
	s := "Hello World"
	for i := 0; i < N; i++ {
		b := []byte(s)
		_ = b[0]
	}
}

/*
BenchmarkFastString2Bytes-12    	2000000000	         0.02 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkFastString2Bytes(b *testing.B) {
	s := "Hello World"
	for i := 0; i < N; i++ {
		b := String2Bytes(s)
		_ = b[0]
	}
}
