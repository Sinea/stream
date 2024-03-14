package stream

// ErrWriteConflict is returned when a transaction write collides with a transaction that is committed
// The caller should Wait on this
type ErrWriteConflict struct {
	message string
	waiter  Waiter
}

func newConflictErr(message string, waiter Waiter) *ErrWriteConflict {
	return &ErrWriteConflict{
		message: message,
		waiter:  waiter,
	}
}

// Error message
func (e *ErrWriteConflict) Error() string {
	return e.message
}

// Wait forwards the call to the wrapped Waiter
func (e *ErrWriteConflict) Wait() {
	e.waiter.Wait()
}
