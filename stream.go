package stream

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var counter = 0

// Sink where to flush buckets of transactions
type Sink[KEY comparable, STATE any] interface {
	Write(ctx context.Context, bucket *Bucket[KEY, STATE]) error
}

// Waiter should block the caller until a condition is met
type Waiter interface {
	Wait()
}

// Stream of data
type Stream[KEY comparable, STATE any] struct {
	once     sync.Once
	commands chan any
	close    chan struct{}
	keys     map[KEY]map[*Transaction[KEY, STATE]]struct{}
	sink     Sink[KEY, STATE]
}

// NewStream creates a new stream
func NewStream[KEY comparable, STATE any](sink Sink[KEY, STATE]) *Stream[KEY, STATE] {
	return &Stream[KEY, STATE]{
		commands: make(chan any),
		close:    make(chan struct{}),
		keys:     make(map[KEY]map[*Transaction[KEY, STATE]]struct{}),
		sink:     sink,
	}
}

// Start the stream
func (s *Stream[KEY, STATE]) Start(ctx context.Context) {
	s.once.Do(func() {
		go s.eventLoop(ctx)
	})
}

// Stop breaks the event loop
func (s *Stream[KEY, STATE]) Stop() {
	close(s.close)
}

// NewTransaction inside this stream
func (s *Stream[KEY, STATE]) NewTransaction(ctx context.Context, linger time.Duration, state STATE) *Transaction[KEY, STATE] {
	counter++
	fmt.Printf("Created transaction %d\n", counter)
	tx := &Transaction[KEY, STATE]{
		ID:       counter,
		state:    state,
		stream:   s,
		result:   make(chan error),
		ctx:      ctx,
		deadline: time.Now().Add(linger),
		cond:     sync.NewCond(&sync.Mutex{}),
	}
	newBucket(s, tx)
	return tx
}

// release keys
func (s *Stream[KEY, STATE]) release(keys []KEY) {
	if len(keys) == 0 {
		return
	}
	s.commands <- &free[KEY]{
		keys: keys,
	}
}

func (s *Stream[KEY, STATE]) eventLoop(ctx context.Context) {
Loop:
	for {
		select {
		case command := <-s.commands:
			s.command(command)
		case <-s.close:
			break Loop
		case <-ctx.Done():
			break Loop
		}
	}
}

func (s *Stream[KEY, STATE]) command(command any) {
	switch concrete := command.(type) {
	case *write[KEY, STATE]:
		s.write(concrete)
	case *commit[KEY, STATE]:
		s.commit(concrete)
	case *rollback[KEY, STATE]:
		s.rollback(concrete)
	case *free[KEY]:
		s.free(concrete)
	default:
		fmt.Printf("Unknown command %KEY", command)
	}
}

func (s *Stream[KEY, STATE]) write(command *write[KEY, STATE]) {
	//fmt.Printf("Writing: %v\n", command.keys)
	defer close(command.result)
	overlappingBuckets := make(map[*Bucket[KEY, STATE]]struct{})
	for i, k := range command.keys {
		batch, reserved := s.keys[k]
		switch {
		case reserved:
			// Requesting transaction already on this key
			if _, ok := batch[command.tx]; ok {
				//fmt.Printf("Already on key %d : %v\n", command.tx.ID, k)
				continue
			}
			// Make sure at least one transaction on this key is still open
			for tx := range batch {
				if !tx.bucket.isOpen() {
					//fmt.Printf("Conflict for %d on key %v\n", command.tx.ID, k)
					s.reset(command.tx, command.keys[:i]...)
					command.result <- newConflictErr("conflict", tx)
					return
				}
				overlappingBuckets[tx.bucket] = struct{}{}
			}
			//fmt.Printf("Append %d to old batch of %v\n", command.tx.ID, k)
			batch[command.tx] = struct{}{} // Add to key pending transactions
		default:
			//fmt.Printf("Append %d to new batch of %v\n", command.tx.ID, k)
			s.keys[k] = map[*Transaction[KEY, STATE]]struct{}{
				command.tx: {},
			}
		}
	}
	//fmt.Printf("All keys written for %d\n", command.tx.ID)
	// merge overlapping buckets
	startBucket := command.tx.bucket
	for b := range overlappingBuckets {
		startBucket = startBucket.merge(b)
	}
}

// remove the transaction from all its keys and recompute potential buckets that need to be flushed
func (s *Stream[KEY, STATE]) rollback(command *rollback[KEY, STATE]) {
	var remainingKeys []KEY
	for _, k := range command.tx.keys {
		batch, ok := s.keys[k]
		if !ok {
			continue
		}
		// Remove myself from the key
		delete(batch, command.tx)
		switch {
		case len(batch) == 0: // I'm the last one
			delete(s.keys, k)
		default: // Will check other transactions
			remainingKeys = append(remainingKeys, k)
		}
	}

	// Rebuild potential buckets starting from remainingKeys
	visitedTx := map[*Transaction[KEY, STATE]]struct{}{}
	visitedKeys := map[KEY]struct{}{}
	newBuckets := map[KEY][]*Transaction[KEY, STATE]{}
	for _, k := range remainingKeys {
		var t []*Transaction[KEY, STATE]
		batch := s.keys[k]
		for tx := range batch {
			s.scan(tx, visitedTx, visitedKeys, &t)
			newBuckets[k] = t
			break
		}
	}

	// Rebuild new buckets
	for _, txs := range newBuckets {
		bucket := newBucket(s, txs...)
		if !bucket.isOpen() {
			if err := s.sink.Write(bucket.context(), bucket); err != nil {
				bucket.resolve(err)
			}
		}
	}
}

// scan for sibling transactions beginning from start. Traversal based on the transaction keys. This will give us all transactions that overlap
func (s *Stream[KEY, STATE]) scan(start *Transaction[KEY, STATE], visitedTx map[*Transaction[KEY, STATE]]struct{}, visitedKeys map[KEY]struct{}, dst *[]*Transaction[KEY, STATE]) {
	for _, k := range start.keys {
		if _, ok := visitedKeys[k]; ok {
			continue
		}
		visitedKeys[k] = struct{}{}
		batch := s.keys[k]
		for tx := range batch {
			if _, ok := visitedTx[tx]; ok {
				continue
			}
			visitedTx[tx] = struct{}{}
			*dst = append(*dst, tx)
			s.scan(tx, visitedTx, visitedKeys, dst)
		}
	}
}

// Commit one transaction. If it's the last one from its bucket the bucket gets flushed
func (s *Stream[KEY, STATE]) commit(command *commit[KEY, STATE]) {
	command.tx.bucket.commitCount++
	if !command.tx.bucket.isOpen() {
		if err := s.sink.Write(command.tx.bucket.context(), command.tx.bucket); err != nil {
			command.tx.bucket.resolve(err)
		}
	}
}

// free up keys. This resolves all transactions on these keys and clears up any empty key
func (s *Stream[KEY, STATE]) free(cmd *free[KEY]) {
	visited := make(map[*Transaction[KEY, STATE]]struct{})
	for _, k := range cmd.keys {
		bundle, exists := s.keys[k]
		if !exists {
			continue
		}
		for tx := range bundle {
			delete(bundle, tx)
			if len(bundle) == 0 {
				delete(s.keys, k)
			}
			if _, isVisited := visited[tx]; isVisited {
				continue
			}
			visited[tx] = struct{}{}
			tx.resolve(nil)
		}
	}
}

// Remove tx from the given keys
func (s *Stream[KEY, STATE]) reset(tx *Transaction[KEY, STATE], keys ...KEY) {
	for _, k := range keys {
		if _, ok := s.keys[k]; ok {
			delete(s.keys[k], tx)
			if len(s.keys[k]) == 0 {
				delete(s.keys, k)
			}
		}
	}
}
