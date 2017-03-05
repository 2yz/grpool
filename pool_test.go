package grpool

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TestWorkUnit struct{}

func (u *TestWorkUnit) Run() {}

type TestPanicWorkUnit struct{}

func (u *TestPanicWorkUnit) Run() {
	log.Println("WorkUnit Panic Test:")
	panic("WorkUnit Panic")
}

type TestWrapWorkUnit struct {
	Func func()
}

func (u *TestWrapWorkUnit) Run() {
	u.Func()
}

func TestNewWorkerPool(t *testing.T) {
	NewWorkerPool(0)
	NewWorkerPool(10)
}

func TestWorkerPool_Queue1(t *testing.T) {
	wp := NewWorkerPool(1)
	wp.Start()
	wp.Queue(&TestWorkUnit{})
	time.Sleep(10 * time.Millisecond)
	wp.Queue(&TestWorkUnit{})
	wp.Queue(&TestWorkUnit{})
	wp.Queue(&TestPanicWorkUnit{})
	time.Sleep(10 * time.Millisecond)
	wp.Stop()
	wp.Queue(&TestWorkUnit{})
	wp.Wait()
}

func TestWorkerPool_QueueAndWait1(t *testing.T) {
	wp := NewWorkerPool(1)
	wp.Start()
	wp.QueueAndWait(&TestPanicWorkUnit{})
	wp.QueueAndWait(&TestPanicWorkUnit{})
	wp.StopAndWait()
}

// test wait tick
func TestWorkerPool_QueueAndWait2(t *testing.T) {
	wp := NewWorkerPool(1)
	wp.Start()
	wp.QueueAndWait(&TestWrapWorkUnit{
		Func: func() {
			time.Sleep(20 * time.Millisecond)
		},
	})
	wp.QueueAndWait(&TestWorkUnit{})
	go wp.QueueAndWait(&TestWorkUnit{})
	go wp.Stop()
	wp.QueueAndWait(&TestWorkUnit{})
	wp.Wait()
}

func TestWorkerPool_clearWorkerQueue(t *testing.T) {
	wp := NewWorkerPool(10)
	for i := 0; i < 10; i++ {
		wp.Queue(&TestWrapWorkUnit{
			Func: func() {},
		})
	}
	wp.StopAndWait()
}

func TestWorkerPool_isRelease(t *testing.T) {
	wp := NewWorkerPool(1)
	wp.Start()
	wp.workerQueue <- wp.newWorker()
	w := wp.newWorker()
	wp.isRelease(w)
}

func TestWorkerPoolParallel(t *testing.T) {
	var wg sync.WaitGroup
	var ops uint64 = 0
	n := runtime.NumCPU()
	wp := NewWorkerPool(n)
	wp.Start()
	for i := 0; i < n*2; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 10000; j++ {
				err := wp.QueueAndWait(&TestWrapWorkUnit{
					Func: func() {
						atomic.AddUint64(&ops, 1)
					},
				})
				if err != nil {
					t.Error(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	wp.StopAndWait()
	result := atomic.CompareAndSwapUint64(&ops, uint64(n*2*10000), uint64(0))
	if !result {
		t.Error("Some workunits miss, the number of finished units is ", ops)
	}
}

func TestWorkerPoolRestart(t *testing.T) {
	wp := NewWorkerPool(1)
	wp.Start()
	wp.QueueAndWait(&TestPanicWorkUnit{})
	wp.StopAndWait()
	wp.Start()
	wp.QueueAndWait(&TestPanicWorkUnit{})
	wp.Stop()
	wp.Start()
	wp.QueueAndWait(&TestPanicWorkUnit{})
	wp.StopAndWait()
}

func BenchmarkWorkerPool(b *testing.B) {
	wp := NewWorkerPool(runtime.NumCPU())
	wp.Start()
	for i := 0; i < b.N; i++ {
		wp.QueueAndWait(&TestWorkUnit{})
	}
	wp.StopAndWait()
}
