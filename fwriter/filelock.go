package fwriter

import (
	"errors"
	"os"
	"syscall"
)

// FLock 文件锁
type FLock struct {
	fn string
	fd int
}

// NewFLock 新建FLock对象.
func NewFLock(fn string) *FLock {
	return &FLock{
		fn: fn + ".lock",
	}
}

// File 返回文件锁保护的文件.
func (l *FLock) File() string {
	return l.fn
}

// Acquire 获取文件锁.
func (l *FLock) Acquire() error {
	if err := l.open(); err != nil {
		return err
	}
	// 非阻塞互斥锁
	if err := syscall.Flock(l.fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		// 需要关闭由多余的Flock操作打开的文件句柄
		if _err := syscall.Close(l.fd); _err != nil {
			return _err
		}
		if err == syscall.EWOULDBLOCK {
			return errors.New("file has been locked by another thread")
		}
	}
	return nil
}

func (l *FLock) open() error {
	fd, err := syscall.Open(l.fn, syscall.O_CREAT|syscall.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	l.fd = fd
	return nil
}

// Release 释放文件锁.
func (l *FLock) Release() error {
	return syscall.Close(l.fd)
}

// Remove 删除文件锁保护的文件.
func (l *FLock) Remove() error {
	return os.Remove(l.fn)
}
