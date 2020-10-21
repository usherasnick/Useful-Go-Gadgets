package tasklb

import (
	"fmt"
	"math"
	"sync"
)

type WorkerUnit interface {
	Name() string
	Do(interface{}) error // change to yourself Do method
}

// TaskLB 用于分配任务执行单元的负载均衡器
type TaskLB struct {
	mu        sync.Mutex
	looktable map[string]int32      // worker name ==> usage
	pool      map[string]WorkerUnit // worker name ==> worker
}

// NewTaskLB 新建TaskLB实例.
func NewTaskLB(limit uint32) *TaskLB {
	return &TaskLB{
		looktable: make(map[string]int32, limit),
		pool:      make(map[string]WorkerUnit, limit),
	}
}

// AddWorker 往负载均衡器中添加任务执行单元.
func (lb *TaskLB) AddWorker(worker WorkerUnit) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if _, ok := lb.looktable[worker.Name()]; ok {
		return fmt.Errorf("worker %s has been added before", worker.Name())
	}

	lb.looktable[worker.Name()] = 0
	lb.pool[worker.Name()] = worker

	return nil
}

// RemoveWorker 从负载均衡器内移除任务执行单元.
func (lb *TaskLB) RemoveWorker(worker WorkerUnit) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if _, ok := lb.looktable[worker.Name()]; !ok {
		return fmt.Errorf("worker %s has not been added before", worker.Name())
	}

	delete(lb.looktable, worker.Name())
	delete(lb.pool, worker.Name())

	return nil
}

// RentWorker 从负载均衡器内获取可用的任务执行单元.
func (lb *TaskLB) RentWorker() WorkerUnit {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	var selected string
	var minUsage = int32(math.MaxInt32)
	for w, usage := range lb.looktable {
		if usage < minUsage {
			selected = w
			minUsage = usage
		}
	}
	lb.looktable[selected]++

	return lb.pool[selected]
}

// RevertWorker 归还任务执行单元.
func (lb *TaskLB) RevertWorker(worker WorkerUnit) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.looktable[worker.Name()]--
	if lb.looktable[worker.Name()] < 0 {
		lb.looktable[worker.Name()] = 0
	}
}
