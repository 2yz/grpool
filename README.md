# grpool
[![Travis](https://img.shields.io/travis/Yezersky/grpool.svg?style=flat-square)](https://travis-ci.org/Yezersky/grpool)
[![Coveralls](https://img.shields.io/coveralls/Yezersky/grpool.svg?style=flat-square)](https://coveralls.io/github/Yezersky/grpool)

A simple goroutine worker pool.

## Install

```
$ go get github.com/yezersky/grpool

# use package management, such as glide
$ glide get github.com/yezersky/grpool
```

## Usage

```go
package main

import (
    "fmt"
    "github.com/yezersky/grpool"
)

// define your work unit
type MyWorkUnit struct {
    // ...
}

// implement WorkUnit interface
func (u *MyWorkUnit) Run() {
    // do something
}

// or wrap any function in WorkUnit
type WrapWorkUnit struct {
    Func func()
}

func (u *WrapWorkUnit) Run() {
    if u.Func == nil {
        return
    }
    u.Func()
}

func main() {
    wp, _ := grpool.NewWorkerPool(2)
    wp.Start()

    // queue work unit immediately
    // if no worker is available, it returns an error
    err := wp.Queue(&MyWorkUnit{})
    if err != nil {
        // ...
    }

    // queue work unit and wait for available worker
    // (not wait to finish work unit)
    err = wp.QueueAndWait(&MyWorkUnit{})
    if err != nil {
        // ...
    }

    // queue a function by wrapping it with a work unit
    wp.QueueAndWait(&WrapWorkUnit{
        Func: func() {
            fmt.Println("function in WarpWorkUnit")
        },
    })

    // work unit can't be queued after stop
    wp.Stop()
    // wait to release all worker
    wp.Wait()
}
```

