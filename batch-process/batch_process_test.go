package batchprocess

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func defaultBatcherGroupCfg() *BatcherGroupCfg {
	return &BatcherGroupCfg{
		BatcherNum: 4,
	}
}

type executor struct{}

func (e *executor) run(item *BatchItem) error {
	return nil
}

func TestBatcherGroup(t *testing.T) {
	testCases := make([]SourceItem, 1024)
	for i := 0; i < 1024; i++ {
		testCases[i] = SourceItem{
			key:  fmt.Sprintf("key-%06d", i),
			item: fmt.Sprintf("job-%06d", i),
		}
	}
	e := &executor{}

	bg := NewBatcherGroup(defaultBatcherGroupCfg())
	bg.Start(e.run)

	for _, tc := range testCases {
		err := bg.Put(tc.key, tc.item)
		assert.Empty(t, err)
	}

	time.Sleep(time.Second * time.Duration(5))

	bg.Stat()
	bg.Close()

	// TODO: FIX ME!!! Totally processed batched requests is 1048!!!
}
