package main

import (
	"context"
)

type Pool interface {
	Get(context.Context) bool
	Put()
}

type NoPool struct{}

func (p *NoPool) Get(context.Context) bool { return true }
func (p *NoPool) Put()                     {}

type JobPool struct {
	ch chan struct{}
}

func (p *JobPool) Get(ctx context.Context) bool {
	select {
	case <-p.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *JobPool) Put() {
	p.ch <- struct{}{}
}

func newJobPool(thread int) Pool {
	if thread == 0 {
		return &NoPool{}
	}
	p := &JobPool{
		ch: make(chan struct{}, thread),
	}
	for range thread {
		p.Put()
	}
	return p
}
