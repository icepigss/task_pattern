package pattern1

import (
	"fmt"
	"sync"
	"time"
)

type worker struct {
	f      *Flow
	exitCh chan interface{}
}

type merger struct {
	f *Flow

	submitCh chan interface{}

	mexitCh chan interface{}
}

type Flow struct {
	w []*worker
	m *merger

	msgCh chan interface{}

	n int

	started bool

	closeCh chan struct{}

	workFn func(interface{}) interface{}

	mergeFn func(interface{})

	wgW sync.WaitGroup
	wgM sync.WaitGroup
}

func NewFlow(n int) *Flow {
	f := &Flow{}
	f.w = make([]*worker, 0, n)
	f.m = newMerger(f)
	f.closeCh = make(chan struct{})
	f.msgCh = make(chan interface{}, 100)
	f.wgW = sync.WaitGroup{}
	f.wgM = sync.WaitGroup{}
	f.n = n

	return f
}

func (f *Flow) Start() {
	go func() {
		for i := 0; i < f.n; i++ {
			f.wgW.Add(1)
			w := newWorker(f)
			f.w = append(f.w, w)
			go w.workLoop(i)
		}
		f.wgM.Add(1)
		go f.m.mergeLoop()

		for {
			select {
			case <-f.closeCh:
				goto exit
			default:
			}
		}

	exit:
		println("all exit")
	}()
}

func (f *Flow) Row(n int) {
	for {
		if f.started {
			break
		}
		if len(f.w) >= f.n {
			f.started = true
		}
		fmt.Printf("worker starting... %d/%d\n", len(f.w), f.n)
		time.Sleep(time.Second)
	}

	f.msgCh <- n
}

func newWorkerPool(n int, f *Flow) []*worker {
	workers := make([]*worker, 0, n)
	for i := 0; i < n; i++ {
		workers = append(workers, newWorker(f))
	}

	return workers
}

func newWorker(f *Flow) *worker {
	w := &worker{}
	w.f = f
	w.exitCh = make(chan interface{})

	return w
}

func newMerger(f *Flow) *merger {
	m := &merger{}
	m.f = f
	m.submitCh = make(chan interface{}, 10)
	m.mexitCh = make(chan interface{})

	return m
}

func (w *worker) workLoop(i int) {
	for {
		select {
		case <-w.exitCh:
			goto exit
		case payload := <-w.f.msgCh:
			if w.f.workFn != nil {
				w.f.m.submitCh <- w.f.workFn(payload)
			}
		}
	}

exit:

	fmt.Printf("worker %d exit.\n", i)

	w.f.wgW.Done()
}

func (f *Flow) Close() {
	for _, w := range f.w {
		w.close()
	}

	f.wgW.Wait()

	close(f.msgCh)
	for payload := range f.msgCh {
		fmt.Printf("left msg, payload: %d.\n", payload)
		f.m.submitCh <- payload
	}

	f.m.close()
	f.wgM.Wait()

	close(f.closeCh)
}

func (w *worker) close() {
	close(w.exitCh)
}

func (m *merger) close() {
	close(m.mexitCh)
}

func (m *merger) mergeLoop() {
	for {
		select {
		case <-m.mexitCh:
			goto exit
		case payload := <-m.submitCh:
			if m.f.mergeFn != nil {
				m.f.mergeFn(payload)
			}
		}
	}

exit:
	close(m.submitCh)
	for payload := range m.submitCh {
		m.f.mergeFn(payload)
	}

	fmt.Printf("merge exit.\n")

	m.f.wgM.Done()
}

func (f *Flow) SetWorkFn(fn func(interface{}) interface{}) {
	f.workFn = fn
}

func (f *Flow) SetMergeFn(fn func(interface{})) {
	f.mergeFn = fn
}
