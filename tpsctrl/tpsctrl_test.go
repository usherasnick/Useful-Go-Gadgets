package tpsctrl

import (
	"sync"
	"testing"
	"time"
)

func TestTPSController(t *testing.T) {
	ctrl := NewTPSController(8)

	wg := new(sync.WaitGroup)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctrl.Take()
			// do something
			time.Sleep(time.Second)
		}(i)
	}
	wg.Wait()
}
