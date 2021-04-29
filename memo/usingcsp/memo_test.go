package usingcsp

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"
)

func httpGetBody(url string) (interface{}, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func TestMemo(t *testing.T) {
	msgs := make(chan string)
	errs := make(chan error)
	done := make(chan struct{})

	memo := New(httpGetBody)
	urls := []string{
		"https://github.com/usherasnick/go-snippet/",
		"https://github.com/usherasnick/go-snippet/",
		"https://github.com/usherasnick/go-snippet/",
		"https://github.com/usherasnick/go-snippet/",
		"https://github.com/usherasnick/go-snippet/",
	}

	var n sync.WaitGroup
	for _, url := range urls {
		n.Add(1)
		go func(url string) {
			defer n.Done()
			start := time.Now()
			val, err := memo.Get(url)
			if err != nil {
				errs <- err
				return
			}
			msgs <- fmt.Sprintf("%s, %s, %d bytes", url, time.Since(start), len(val.([]byte)))
		}(url)
	}

	go func() {
		n.Wait()
		close(done)
	}()

loop:
	for {
		select {
		case msg := <-msgs:
			t.Log(msg)
		case err := <-errs:
			t.Log(err)
		case <-done:
			break loop
		}
	}
}
