package tasklb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type printer struct {
	name string
}

func (p *printer) Name() string {
	return p.name
}

func (p *printer) Do(content interface{}) error {
	fmt.Printf("[printer %s] %s\n", p.name, content)
	time.Sleep(time.Second)
	return nil
}

func TestTaskLB(t *testing.T) {
	lb := NewTaskLB(8)

	for i := 0; i < 8; i++ {
		err := lb.AddWorker(&printer{name: fmt.Sprintf("hp-%d", i+1)})
		assert.Empty(t, err)
	}

	wg := new(sync.WaitGroup)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			w := lb.RentWorker()
			w.Do("hello world") // nolint
			lb.RevertWorker(w)
		}(i)
	}
	wg.Wait()
}
