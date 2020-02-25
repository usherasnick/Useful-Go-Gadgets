package streamop

import (
	"sync/atomic"
)

// FanOut 信号通知模式, 扇出模式.
// 常用于观察者模式中, 上游/下游服务状态变动后, 多个观察者都会接收到变更通知信号.
func FanOut(in <-chan interface{}, out []chan interface{}, async bool) {
	go func() {
		var finish atomic.Value

		defer func() { // 退出时关闭所有的输出channel
			for async && finish.Load().(int) < len(out) {
			}

			for i := 0; i < len(out); i++ {
				close(out[i])
			}
		}()

		for v := range in {
			if async {
				finish.Store(0)
			}

			vv := v
			for i := 0; i < len(out); i++ {
				ii := i
				// TODO: 异步模式下, 会出现向已关闭的channel写数据
				// Done: 利用原子计数来修复
				if async {
					go func() {
						out[i] <- vv
						finish.Store(finish.Load().(int) + 1)
					}()
				} else {
					out[ii] <- vv
				}
			}
		}
	}()
}
