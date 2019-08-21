package fwriter

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SafeWriter 文件读写器 (线程安全).
type SafeWriter struct {
	flock     *FLock
	writer    *os.File
	fn        string
	tmpSuffix string
}

// NewSafeWriter 新建SafeWriter对象.
func NewSafeWriter(fn string) (*SafeWriter, error) {
	if err := os.MkdirAll(filepath.Dir(fn), 0750); err != nil {
		return nil, err
	}

	flock := NewFLock(fn)
	if err := flock.Acquire(); err != nil {
		return nil, err
	}

	tmpSuffix := fmt.Sprintf(".tmp%v", time.Now().UnixNano())

	writer, err := os.OpenFile(fn+tmpSuffix, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		flock.Release() // nolint
		flock.Remove()  // nolint
		return nil, err
	}

	return &SafeWriter{
		flock:     flock,
		writer:    writer,
		fn:        fn,
		tmpSuffix: tmpSuffix,
	}, nil
}

// Write 写字节流.
func (w *SafeWriter) Write(content []byte) (int, error) {
	return w.writer.Write(content)
}

// WriteString 写字符串.
func (w *SafeWriter) WriteString(content string) (int, error) {
	return w.writer.WriteString(content)
}

// Commit 持久化内存数据到硬盘.
func (w *SafeWriter) Commit() error {
	defer w.exit()
	if err := w.writer.Sync(); err != nil {
		return err
	}
	if err := os.Rename(w.fn+w.tmpSuffix, w.fn); err != nil {
		return err
	}
	return nil
}

// Abort 放弃当前写操作.
func (w *SafeWriter) Abort() {
	w.exit()
}

func (w *SafeWriter) exit() {
	w.writer.Close() // nolint
	w.unlock()
	os.Remove(w.fn + w.tmpSuffix) // nolint
}

func (w *SafeWriter) unlock() {
	w.flock.Release() // nolint
	w.flock.Remove()  // nolint
}
