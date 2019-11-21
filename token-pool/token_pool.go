package tokenpool

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type TokenPool struct {
	mu sync.Mutex

	concurrency int
	q           chan int
	pend        int
}

func NewTokenPool(concurrency int) *TokenPool {
	p := TokenPool{
		concurrency: concurrency,
		q:           make(chan int, concurrency+1),
		pend:        0,
	}
	for i := 0; i < concurrency; i++ {
		p.q <- i
		p.pend++
	}
	return &p
}

func (p *TokenPool) Acquire(ctx context.Context) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-ctx.Done():
		{
			return -1, ctx.Err()
		}
	case token, ok := <-p.q:
		{
			if !ok {
				return -1, fmt.Errorf("pool has been closed")
			}
			p.pend--
			return token, nil
		}
	}
}

func (p *TokenPool) Release(token int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.q <- token
	p.pend++
}

func (p *TokenPool) Join() {
	for {
		time.Sleep(time.Millisecond * 100)
		p.mu.Lock()
		if p.pend == p.concurrency { // wait until all workers to release token
			time.Sleep(time.Second * 1)
			if p.pend == p.concurrency { // double check
				p.mu.Unlock()
				return
			}
		}
		p.mu.Unlock()
	}
}
