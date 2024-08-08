package utils

import "sync"

type Closer struct {
	waiting     sync.WaitGroup
	CloseSignal chan struct{}
}

func NewCloser() *Closer {
	return &Closer{
		waiting:     sync.WaitGroup{},
		CloseSignal: make(chan struct{}),
	}
}

func (c *Closer) Close() {
	// 发送关闭信号, 此时监听此通道的select被唤醒,去执行关闭逻辑,最后再调用 Done()函数
	close(c.CloseSignal)
	c.waiting.Wait()
}

func (c *Closer) Done() {
	c.waiting.Done()
}

func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}
