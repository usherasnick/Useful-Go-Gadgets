package streamop

import (
	"reflect"
)

// OrDone 一种信号通知模式,
// 如果有多个同类微服务任务同时执行, 只要其中一个任务执行完毕, 就通知上游/下游服务.
func OrDone(notifyChs ...chan struct{}) chan struct{} {
	// 特殊情况, 只有0个或者1个微服务
	switch len(notifyChs) {
	case 0:
		return nil
	case 1:
		return notifyChs[0]
	}

	orDone := make(chan struct{})
	go func() {
		defer close(orDone)

		// 利用反射构建SelectCase
		var cases []reflect.SelectCase
		for _, ch := range notifyChs {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
		}

		// 随机选择一个就绪的case
		reflect.Select(cases)
	}()
	return orDone
}
