package utils

import "sync"

type Throttle struct {
	once      sync.Once
	wg        sync.WaitGroup
	ch        chan struct{}
	errCh     chan error
	finishErr error
}

func NewThrottle(max int) *Throttle {
	return &Throttle{
		once:      sync.Once{},
		wg:        sync.WaitGroup{},
		ch:        make(chan struct{}, max),
		errCh:     make(chan error, max),
		finishErr: nil,
	}
}

func (t *Throttle) Do() error {
	for {
		select {
		case t.ch <- struct{}{}:
			t.wg.Add(1)
			return nil
		case err := <-t.errCh:
			if err != nil {
				return err
			}
		}
	}
}

func (t *Throttle) Done(err error) {
	if err != nil {
		t.errCh <- err
	}
	select {
	case <-t.ch:
	default:
		panic("Throttle Do Done mismatch")
	}
	t.wg.Done()
}

func (t *Throttle) Finish() error {
	t.once.Do(func() {
		t.wg.Wait()
		close(t.ch)
		close(t.errCh)
		for err := range t.errCh {
			if err != nil {
				t.finishErr = err
				return
			}
		}
	})
	return t.finishErr
}
