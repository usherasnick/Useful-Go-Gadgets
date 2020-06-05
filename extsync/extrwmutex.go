package extsync

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const rwmutexMaxReaders = 1 << 30

// RWMutex 高级读写锁
type RWMutex struct {
	sync.RWMutex
}

// TryRWLock 尝试获取读写锁(非阻塞), read == true代表想获取读锁, 否则代表想获取写锁.
func (rw *RWMutex) TryRWLock(read bool) bool {
	readerCntPtr := (*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(&rw)) + unsafe.Sizeof(sync.Mutex{}) + 2*unsafe.Sizeof(uint32(0))))
	readerCnt := atomic.LoadInt32(readerCntPtr)
	if readerCnt < 0 {
		// 被其他写锁持有
		return false
	}

	if read && atomic.CompareAndSwapInt32(readerCntPtr, readerCnt, readerCnt+1) {
		return true
	} else if !read && atomic.CompareAndSwapInt32(readerCntPtr, readerCnt, -rwmutexMaxReaders) {
		rw.Lock()
		return true
	}

	return false
}

// RCount 统计当前等待获取读锁的协程数.
func (rw *RWMutex) RCount() int32 {
	readerCntPtr := (*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(&rw)) + unsafe.Sizeof(sync.Mutex{}) + 2*unsafe.Sizeof(uint32(0))))
	readerCnt := atomic.LoadInt32(readerCntPtr)
	if readerCnt < 0 {
		return atomic.LoadInt32((*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(&rw)) + unsafe.Sizeof(sync.Mutex{}) + 2*unsafe.Sizeof(uint32(0)) + unsafe.Sizeof(int32(0)))))
	}
	return readerCnt
}

// WExist 查看当前是否有持有写锁的协程.
func (rw *RWMutex) WExist() bool {
	return atomic.LoadInt32((*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(&rw))+unsafe.Sizeof(sync.Mutex{})+unsafe.Sizeof(uint32(0))))) < 0
}
