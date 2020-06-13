package extsync

import (
	"testing"
	"time"
)

type fakeCounter struct {
	ReentrantMutex
	Count int
}

var gCounter fakeCounter

func reentrant(t *testing.T, iter int) {
	if iter == 10 {
		return
	}
	gCounter.Lock()
	defer gCounter.Unlock()
	gCounter.Count++
	t.Log(gCounter.Info())
	reentrant(t, iter+1)
}

func TestReentrantMutex(t *testing.T) {
	go reentrant(t, 0)
	time.Sleep(2 * time.Second)
}
