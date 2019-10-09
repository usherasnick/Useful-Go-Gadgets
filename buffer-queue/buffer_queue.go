package bufferqueue

import (
	"sync"

	"github.com/gammazero/deque"
)

type LimitBufferQueue struct {
	mu sync.Mutex

	q    deque.Deque
	cap  int
	cond *sync.Cond
}

func NewLimitBufferQueue(cap int) *LimitBufferQueue {
	if cap == 0 {
		cap = 128
	}
	q := LimitBufferQueue{
		cap: cap,
	}
	q.cond = sync.NewCond(&sync.Mutex{})
	return &q
}

func (q *LimitBufferQueue) BPush(x interface{}) {
	q.cond.L.Lock()
	for q.q.Len() >= q.cap {
		q.cond.Wait()
	}
	defer q.cond.L.Unlock()

	q.q.PushBack(x)
	q.cond.Signal()
}

func (q *LimitBufferQueue) BPop(want int) []interface{} {
	q.cond.L.Lock()
	for q.q.Len() == 0 {
		q.cond.Wait()
	}
	defer q.cond.L.Unlock()

	if q.q.Len() < want {
		want = q.q.Len()
	}

	outputs := make([]interface{}, want)
	for i := 0; i < want; i++ {
		outputs[i] = q.q.PopFront()
	}
	q.cond.Signal()
	return outputs
}

func (q *LimitBufferQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.q.Len()
}
