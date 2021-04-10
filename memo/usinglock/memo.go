package usinglock

import (
	"sync"
)

type Memo struct {
	f     Func
	cache map[string]*entry
	mu    sync.Mutex
}

type Func func(key string) (interface{}, error)

type entry struct {
	res   result
	ready chan struct{}
}

type result struct {
	val interface{}
	err error
}

func New(f Func) *Memo {
	return &Memo{
		f:     f,
		cache: make(map[string]*entry),
	}
}

func (memo *Memo) Get(key string) (interface{}, error) {
	memo.mu.Lock()
	e := memo.cache[key]
	if e == nil {
		/*
			if it's the first time to visit the key, current goroutine will be in charge
			of the job to do the computation and broadcast the done signal.
		*/
		e = &entry{
			ready: make(chan struct{}),
		}
		memo.cache[key] = e
		memo.mu.Unlock()

		e.res.val, e.res.err = memo.f(key)
		close(e.ready) // broadcast the done signal
	} else {
		memo.mu.Unlock()
		<-e.ready // wait until the done signal is received
	}
	return e.res.val, e.res.err
}
