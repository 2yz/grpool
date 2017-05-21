package grpool

import (
	"errors"
	"log"
	"sync"
	"time"
)

const NO_WAIT = false
const WAIT = true

type WorkerPool struct {
	maxWorkerNumber int
	workerCount     int
	workerQueue     chan *Worker
	cachePool       sync.Pool
	cond            *sync.Cond
	mustStop        bool
}

func NewWorkerPool(maxWorkerNumber int) (*WorkerPool, error) {
	if maxWorkerNumber < 1 {
		return nil, errors.New("maxWorkerNumber must be larger than 0")
	}
	wp := &WorkerPool{
		maxWorkerNumber: maxWorkerNumber,
		workerCount:     0,
		workerQueue:     make(chan *Worker, maxWorkerNumber),
		cachePool: sync.Pool{
			New: func() interface{} {
				return &Worker{
					ch: make(chan WorkUnit, 1),
				}
			},
		},
		cond: sync.NewCond(&sync.Mutex{}),
	}
	return wp, nil
}

func (wp *WorkerPool) Start() {
	wp.mustStop = false
}

func (wp *WorkerPool) Stop() {
	wp.cond.L.Lock()
	wp.mustStop = true
	wp.clearWorkerQueue()
	wp.cond.L.Unlock()
}

func (wp *WorkerPool) StopAndWait() {
	wp.Stop()
	wp.Wait()
}

func (wp *WorkerPool) Wait() {
	wp.cond.L.Lock()
	for wp.workerCount != 0 {
		wp.cond.Wait()
	}
	wp.cond.L.Unlock()
}

func (wp *WorkerPool) clearWorkerQueue() {
	for {
		select {
		case w := <-wp.workerQueue:
			w.ch <- nil
		default:
			return
		}
	}
}

func (wp *WorkerPool) Queue(unit WorkUnit) error {
	go wp.queue(unit)
	return nil
}

func (wp *WorkerPool) QueueAndWait(unit WorkUnit) error {
	return wp.queue(unit)
}

func (wp *WorkerPool) queue(unit WorkUnit) error {
	w, err := wp.getWorker()
	if err != nil {
		return err
	}
	w.ch <- unit
	return nil
}

func (wp *WorkerPool) getWorker() (*Worker, error) {
	createWorker := false

	wp.cond.L.Lock()
	if wp.mustStop == true {
		err := errors.New("WorkerPool is stopped")
		wp.cond.L.Unlock()
		return nil, err
	}
	if wp.workerCount < wp.maxWorkerNumber {
		createWorker = true
		wp.workerCount += 1
	}
	wp.cond.L.Unlock()

	if createWorker == true {
		w := wp.newWorker()
		return w, nil
	}

	tick := time.Tick(10 * time.Millisecond)
	for {
		select {
		case w := <-wp.workerQueue:
			return w, nil
		case <-tick:
			wp.cond.L.Lock()
			if wp.mustStop {
				wp.cond.L.Unlock()
				return nil, errors.New("Stopped WorkerPool")
			}
			wp.cond.L.Unlock()
		}
	}
}

func (wp *WorkerPool) newWorker() *Worker {
	vw := wp.cachePool.Get()
	w := vw.(*Worker)
	go wp.runWorker(w)
	return w
}

func (wp *WorkerPool) runWorker(w *Worker) {
	defer wp.endWorker(w)
	var unit WorkUnit
	for unit = range w.ch {
		if unit == nil {
			break
		}
		wp.runUnit(unit)
		wp.cond.L.Lock()
		if wp.isRelease(w) {
			wp.cond.L.Unlock()
			break
		}
		wp.cond.L.Unlock()
	}
}

func (wp *WorkerPool) endWorker(w *Worker) {
	wp.cond.L.Lock()
	wp.workerCount -= 1
	wp.cond.L.Unlock()
	wp.cond.Broadcast()

	wp.cachePool.Put(w)
}

func (wp *WorkerPool) isRelease(w *Worker) bool {
	if wp.mustStop {
		return true
	}
	select {
	case wp.workerQueue <- w:
		return false
	default:
		return true
	}
}

func (wp *WorkerPool) runUnit(unit WorkUnit) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Unit Panic:", err)
		}
	}()
	unit.Run()
}

type WorkUnit interface {
	Run()
}

type WorkUnitChannel chan WorkUnit

type Worker struct {
	ch WorkUnitChannel
}
