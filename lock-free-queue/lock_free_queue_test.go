package lockfreequeue

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLockFreeQueue(t *testing.T) {
	q := NewLockFreeQueue()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			q.Push("Lucy")
			wg.Done()
		}()
	}
	successCh := make(chan int, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			if _, ok := q.Pop(); ok {
				successCh <- 1
			} else {
				successCh <- 0
			}
			wg.Done()
		}()
	}

	stopCh := make(chan struct{})
	go func() {
		succ := 0
		for x := range successCh {
			succ += x
		}
		assert.Equal(t, int64(100-succ), q.Len())
		stopCh <- struct{}{}
	}()

	wg.Wait()
	close(successCh)
	<-stopCh

	// TODO: 测试失败？问题出在哪里？
}
