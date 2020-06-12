package extsync

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/petermattis/goid"
)

// ReentrantMutex 可重入锁
type ReentrantMutex struct {
	sync.Mutex

	owner     int64 // 当前持有锁的goroutine id
	recursion int32 // 重入次数
}

// Info 返回锁的拥有者和重入次数
func (m *ReentrantMutex) Info() string {
	return fmt.Sprintf("owner (%d), recursion (%d)", m.owner, m.recursion)
}

// Lock 可重入锁加锁
func (m *ReentrantMutex) Lock() {
	gid := goid.Get()
	// 如果这个goroutine就是当前持有锁的goroutine, 则允许重入
	if atomic.LoadInt64(&m.owner) == gid {
		// 重入次数加一（非临界区，不需要原子操作）
		m.recursion++
		return
	}
	// 首次获取锁
	m.Mutex.Lock()
	// 首次获取时记录下goroutine id
	atomic.StoreInt64(&m.owner, gid)
	// 首次获取时重入次数置为1（非临界区，不需要原子操作）
	m.recursion = 1
}

// Unlock 可重入锁解锁
func (m *ReentrantMutex) Unlock() {
	gid := goid.Get()
	// 非持有锁的goroutine尝试释放锁, 并不允许释放
	if atomic.LoadInt64(&m.owner) != gid {
		panic(fmt.Sprintf("goroutine (%d) does not hold the current lock, but the owner (%d) does", m.owner, gid))
	}
	// 重入次数减1
	m.recursion--
	// 如果这个goroutine还未完全释放锁, 则直接返回
	if m.recursion != 0 {
		return
	}
	// 此goroutine最后一次调用, 需要释放锁
	atomic.StoreInt64(&m.owner, -1)
	m.Mutex.Unlock()
}
