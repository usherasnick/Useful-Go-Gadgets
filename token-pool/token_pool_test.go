package tokenpool

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicJob(t *testing.T) {
	ctx := context.Background()
	p := NewTokenPool(10)

	token, err := p.Acquire(ctx)
	assert.Empty(t, err)
	assert.Equal(t, 0, token)

	p.Release(token)
}

func TestComplexJobWithoutJoin(t *testing.T) {
	ctx := context.Background()
	p := NewTokenPool(10)

	wg := new(sync.WaitGroup)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			token, err := p.Acquire(ctx)
			assert.Empty(t, err)
			p.Release(token)
		}(i)
	}
	wg.Wait()
}

func TestComplexJobWithJoin(t *testing.T) {
	ctx := context.Background()
	p := NewTokenPool(10)

	for i := 0; i < 100; i++ {
		go func(idx int) {
			token, err := p.Acquire(ctx)
			assert.Empty(t, err)
			p.Release(token)
		}(i)
	}

	p.Join()
}
