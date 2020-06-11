package extsync

import (
	"sync"
	"sync/atomic"
)

// Once 一个功能更加强大的Once
type Once struct {
	done uint32
	m    sync.Mutex
}

// Do 传入的函数f带返回值error, 如果初始化失败, 需要返回error
func (o *Once) Do(f func() error) error {
	if atomic.LoadUint32(&o.done) == 1 {
		// fast path
		return nil
	}
	return o.doSlow(f)
}

func (o *Once) doSlow(f func() error) error {
	o.m.Lock()
	defer o.m.Unlock()
	var err error
	if o.done == 0 { // double-checking, 还未初始化
		if err = f(); err == nil {
			// 初始化成功才将标记位设置为已初始化
			atomic.StoreUint32(&o.done, 1)
		}
	}
	return err
}
