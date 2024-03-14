package stream

import (
	"context"
	"time"
)

// Bucket of transactions
type Bucket[KEY comparable, STATE any] struct {
	// parent stream
	stream *Stream[KEY, STATE]
	// transactions in this bucket
	transactions []*Transaction[KEY, STATE]
	// deadline is the minimum from all transactions in this bucket
	deadline time.Time
	// number of transactions that are committed
	commitCount int
}

// Release the keys on which the transactions in this bucket have a lock
func (b *Bucket[KEY, STATE]) Release() {
	b.stream.release(b.Keys())
}

// newBucket creates a new transaction bucket with the deadline being the minimum deadline from all transactions
func newBucket[KEY comparable, STATE any](stream *Stream[KEY, STATE], transactions ...*Transaction[KEY, STATE]) *Bucket[KEY, STATE] {
	bucket := &Bucket[KEY, STATE]{
		stream:       stream,
		transactions: transactions,
		deadline:     time.Now(),
		commitCount:  0,
	}
	for _, t := range transactions {
		if t.deadline.Before(bucket.deadline) {
			bucket.deadline = t.deadline
		}
		if t.status.Load() == Committed {
			bucket.commitCount++
		}
		t.bucket = bucket
	}
	return bucket
}

// merge two buckets
func (b *Bucket[KEY, STATE]) merge(other *Bucket[KEY, STATE]) *Bucket[KEY, STATE] {
	deadline := b.deadline
	if other.deadline.Before(deadline) {
		deadline = other.deadline
	}
	bucket := &Bucket[KEY, STATE]{
		deadline: deadline,
		stream:   b.stream,
	}
	visited := make(map[*Transaction[KEY, STATE]]struct{})
	for _, tx := range append(b.transactions, other.transactions...) {
		if _, ok := visited[tx]; ok {
			continue
		}
		tx.bucket = bucket
		if tx.status.Load() == Committed {
			bucket.commitCount++
		}
		visited[tx] = struct{}{}
		bucket.transactions = append(bucket.transactions, tx)
	}
	return bucket
}

// isOpen for more admissions
func (b *Bucket[KEY, STATE]) isOpen() bool {
	return b.deadline.After(time.Now()) || len(b.transactions) > b.commitCount
}

// context of the bucket is the one which has the earliest deadline
// when no context with deadline is encountered a background context is returned
func (b *Bucket[KEY, STATE]) context() context.Context {
	deadline := time.Now().Add(time.Hour)
	ctx := context.Background()
	for _, tx := range b.transactions {
		d, ok := tx.ctx.Deadline()
		if !ok {
			continue
		}
		if d.Before(deadline) {
			deadline = d
			ctx = tx.ctx
		}
	}

	return ctx
}

func (b *Bucket[KEY, STATE]) resolve(err error) {
	for _, tx := range b.transactions {
		tx.resolve(err)
	}
}

func (b *Bucket[KEY, STATE]) States() []STATE {
	var states []STATE
	for _, tx := range b.transactions {
		states = append(states, tx.state)
	}
	return states
}

func (b *Bucket[KEY, STATE]) Keys() []KEY {
	var keys []KEY
	for _, tx := range b.transactions {
		keys = append(keys, tx.keys...)
	}
	return keys
}
