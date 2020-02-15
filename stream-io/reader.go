package streamio

/* Reading Rules

type Reader interface {
    Read(p []byte) (n int, err error)
}

1. A Read() call will read up to len(p) into p, when possible.
2. After a Read() call, n may be less then len(p).
3. Upon error, a Read() call may still return n bytes in transfer buffer p.
   For instance, reading from a TCP socket that is abruptly closed.
   Depending on your own use, you may choose to keep the bytes in p or just retry.
4. When a Read() call exhausts available data, a reader may return a non-zero n and err=io.EOF.
   However, depending on implementation, a reader may choose to return a non-zero n and err=nil at the end of stream-op.
   In that case, any subsequent read ops must return n=0, err=io.EOF.
5. A Read() call that returns n=0 and err=nil does not mean EOF as the next call to Read() may return more data.

*/
