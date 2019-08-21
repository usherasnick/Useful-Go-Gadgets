package fwriter

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSafeWriter(t *testing.T) {
	w, err := NewSafeWriter("./fixtures/test.txt")
	assert.Empty(t, err)
	_, err = NewSafeWriter("./fixtures/test.txt")
	assert.NotEmpty(t, err)
	t.Log(err)
	n, err := w.WriteString("hello world")
	assert.Empty(t, err)
	assert.Equal(t, 11, n)
	err = w.Commit()
	assert.Empty(t, err)
	err = os.Remove("./fixtures/test.txt")
	assert.Empty(t, err)
}
