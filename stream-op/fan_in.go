package streamop

import (
	"reflect"
)

// FanIn 信号通知模式, 扇入模式.
func FanIn(ins ...<-chan interface{}) <-chan interface{} {
	out := make(chan interface{}, len(ins))
	go func() {
		defer close(out)

		// 利用反射构建SelectCase
		var cases []reflect.SelectCase
		for _, ch := range ins {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
		}

		// 循环执行, 随机选择一个就绪的case
		for len(cases) > 0 {
			i, v, ok := reflect.Select(cases)
			if !ok { // 表明此channel已经关闭
				cases = append(cases[:i], cases[i+1:]...)
				continue
			}
			out <- v.Interface()
		}
	}()
	return out
}
