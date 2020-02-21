package streamop

import (
	"context"
)

// AsStream 创建流.
func AsStream(ctx context.Context, values ...interface{}) <-chan interface{} {
	stream := make(chan interface{})
	go func() {
		defer close(stream)

		for _, v := range values {
			select {
			case <-ctx.Done():
				return
			case stream <- v:
			}
		}
	}()
	return stream
}

// TakeN 只取流中前N个数据.
func TakeN(ctx context.Context, valueStream <-chan interface{}, N int) <-chan interface{} {
	hijackStream := make(chan interface{})
	go func() {
		defer close(hijackStream)

		for i := 0; i < N; i++ { // 只读取前N个元素
			select {
			case <-ctx.Done():
				return
			case v, ok := <-valueStream:
				if !ok {
					return
				}
				hijackStream <- v
			}
		}
	}()
	return hijackStream
}

// SkipN 跳过流中前N个数据.
func SkipN(ctx context.Context, valueStream <-chan interface{}, N int) <-chan interface{} {
	hijackStream := make(chan interface{})
	go func() {
		defer close(hijackStream)

		for i := 0; i < N; i++ { // 只跳过前N个元素
			select {
			case <-ctx.Done():
				return
			case <-valueStream:
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-valueStream:
				if !ok {
					return
				}
				hijackStream <- v
			}
		}
	}()
	return hijackStream
}

// TakeFn 筛选流中数据, 保留满足条件的数据.
func TakeFn(ctx context.Context, valueStream <-chan interface{}, fn func(v interface{}) bool) <-chan interface{} {
	hijackStream := make(chan interface{})
	go func() {
		defer close(hijackStream)

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-valueStream:
				{
					if !ok {
						return
					}
					if fn(v) {
						hijackStream <- v
					}
				}
			}
		}
	}()
	return hijackStream
}

// SkipFn 筛选流中数据, 跳过满足条件的数据.
func SkipFn(ctx context.Context, valueStream <-chan interface{}, fn func(v interface{}) bool) <-chan interface{} {
	hijackStream := make(chan interface{})
	go func() {
		defer close(hijackStream)

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-valueStream:
				{
					if !ok {
						return
					}
					if !fn(v) {
						hijackStream <- v
					}
				}
			}
		}
	}()
	return hijackStream
}

// TakeWhile 只取前面满足条件的数据, 一旦不满足条件, 就不再读取.
func TakeWhile(ctx context.Context, valueStream <-chan interface{}, fn func(v interface{}) bool) <-chan interface{} {
	hijackStream := make(chan interface{})
	go func() {
		defer close(hijackStream)

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-valueStream:
				{
					if !ok {
						return
					}
					if !fn(v) {
						return
					}
					hijackStream <- v
				}
			}
		}
	}()
	return hijackStream
}

// SkipWhile 跳过前面满足条件的数据, 一旦不满足条件, 就开始读取.
func SkipWhile(ctx context.Context, valueStream <-chan interface{}, fn func(v interface{}) bool) <-chan interface{} {
	hijackStream := make(chan interface{})
	go func() {
		defer close(hijackStream)

		skip := true

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-valueStream:
				{
					if !ok {
						return
					}
					if skip && !fn(v) {
						skip = false
					}

					if !skip {
						hijackStream <- v
					}
				}
			}
		}
	}()
	return hijackStream
}
