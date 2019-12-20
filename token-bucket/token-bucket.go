package tokenbucket

import "context"

type TokenBucket struct {
	tokens chan struct{}
}

func NewTokenBucket(cap int) *TokenBucket {
	return &TokenBucket{
		tokens: make(chan struct{}, cap),
	}
}

func (b *TokenBucket) Take() {
	b.tokens <- struct{}{}
}

func (b *TokenBucket) TakeWithCtx(ctx context.Context) {
	select {
	case <-ctx.Done():
	case b.tokens <- struct{}{}:
	}
}

func (b *TokenBucket) GiveBack() {
	<-b.tokens
}
