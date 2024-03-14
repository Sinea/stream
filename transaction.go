package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNoKeys           = errors.New("no keys provided")
	ErrClosed           = errors.New("transaction closed")
	ErrAlreadyCommitted = errors.New("transaction already committed")
)

const (
	Committed int32 = 1 + iota
	Closed
)

type Transaction[KEY comparable, STATE any] struct {
	ID     int
	stream *Stream[KEY, STATE]
	keys   []KEY
	state  STATE
	//isCommitted bool
	result   chan error
	ctx      context.Context
	deadline time.Time
	bucket   *Bucket[KEY, STATE]
	cond     *sync.Cond
	status   atomic.Int32
}

func (t *Transaction[KEY, STATE]) Keys() []KEY {
	return t.keys
}

func (t *Transaction[KEY, STATE]) Write(keys ...KEY) error {
	if len(keys) == 0 {
		return ErrNoKeys
	}
	state := t.status.Load()
	switch state {
	case Committed:
		return ErrAlreadyCommitted
	case Closed:
		return ErrClosed
	default:
	}

	//fmt.Printf("Write transaction %d: %v\n", t.ID, keys)

	cmd := &write[KEY, STATE]{
		ctx:    t.ctx,
		tx:     t,
		result: make(chan error, 1),
		keys:   keys,
	}

	err := t.submit(t.ctx, cmd, cmd.result)
	if err == nil {
		t.keys = append(t.keys, keys...)
	}
	return err
}

func (t *Transaction[KEY, STATE]) Rollback() {
	if t.status.Swap(Closed) == Closed {
		return
	}
	t.stream.commands <- &rollback[KEY, STATE]{
		tx: t,
	}
}

func (t *Transaction[KEY, STATE]) Commit() error {
	if t.status.Load() == Closed {
		return ErrClosed
	}
	if t.status.Swap(Committed) == Committed {
		return ErrAlreadyCommitted
	}
	//fmt.Printf("Commit transaction %d\n", t.ID)
	cmd := &commit[KEY, STATE]{
		result: t.result,
		tx:     t,
	}
	return t.submit(t.ctx, cmd, cmd.result)
}

// Wait blocks the caller until this transaction is resolved
func (t *Transaction[KEY, STATE]) Wait() {
	for t.status.Load() != Closed {
		t.cond.L.Lock()
		t.cond.Wait()
		t.cond.L.Unlock()
	}
}

func (t *Transaction[KEY, STATE]) submit(ctx context.Context, cmd any, results chan error) error {
	// Wait for command admission
	select {
	case t.stream.commands <- cmd:
	case <-ctx.Done():
		return fmt.Errorf("error waiting for command admission: %w", ctx.Err())
	}

	// Wait for command result
	select {
	case err := <-results:
		return err
	case <-ctx.Done():
		return fmt.Errorf("error waiting for command result: %w", ctx.Err())
	}
}

func (t *Transaction[KEY, STATE]) resolve(err error) {
	if t.status.Swap(Closed) == Closed {
		return
	}
	if err != nil {
		t.result <- err
	}
	close(t.result)
	t.cond.Broadcast()
}
