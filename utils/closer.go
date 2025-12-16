package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync"
)

type Closer struct {
	waiting     sync.WaitGroup
	CloseSignal chan struct{}
}

func NewCloser(add int) *Closer {
	closer := &Closer{}
	closer.waiting.Add(add)
	closer.CloseSignal = make(chan struct{})
	return closer
}

func (c *Closer) CloseAndWait() {
	// 发送关闭信号, 此时监听此通道的select被唤醒,去执行关闭逻辑,最后再调用 Done()函数;
	close(c.CloseSignal)
	c.waiting.Wait()
}

func (c *Closer) Done() {
	c.waiting.Done()
}

func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}
