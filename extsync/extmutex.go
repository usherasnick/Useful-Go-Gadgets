package extsync

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	mutexLocked = 1 << iota // mutex is locked
	mutexWoken
	mutexStarving
	mutexWaiterShift = iota
)

// Mutex 高级互斥锁
type Mutex struct {
	sync.Mutex
}

// TryLock 尝试获取锁(非阻塞).
func (m *Mutex) TryLock() bool {
	if atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&m.Mutex)), 0, mutexLocked) {
		// fast path
		return true
	}
	old := atomic.LoadInt32((*int32)(unsafe.Pointer(&m.Mutex)))
	if old&(mutexLocked|mutexWoken|mutexStarving) != 0 {
		return false
	}
	new := old | mutexLocked
	return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&m.Mutex)), old, new)
}

// Count 统计当前等待获取锁的协程数.
func (m *Mutex) Count() int {
	v := atomic.LoadInt32((*int32)(unsafe.Pointer(&m.Mutex)))
	v = v>>mutexWaiterShift + (v & mutexLocked)
	return int(v)
}

// IsLocked 判断锁当前是否处于被持有状态.
func (m *Mutex) IsLocked() bool {
	v := atomic.LoadInt32((*int32)(unsafe.Pointer(&m.Mutex)))
	return v&mutexLocked == mutexLocked
}

// IsWoken 判断锁当前是否处于被唤醒状态.
func (m *Mutex) IsWoken() bool {
	v := atomic.LoadInt32((*int32)(unsafe.Pointer(&m.Mutex)))
	return v&mutexWoken == mutexWoken
}

// IsStarving 判断锁当前是否处于饥饿状态.
func (m *Mutex) IsStarving() bool {
	v := atomic.LoadInt32((*int32)(unsafe.Pointer(&m.Mutex)))
	return v&mutexStarving == mutexStarving
}
