package stream

import "context"

type write[KEY comparable, STATE any] struct {
	ctx    context.Context
	tx     *Transaction[KEY, STATE]
	keys   []KEY
	result chan error
}

type commit[KEY comparable, STATE any] struct {
	tx     *Transaction[KEY, STATE]
	result chan error
}

type rollback[T comparable, STATE any] struct {
	tx *Transaction[T, STATE]
}

type free[T comparable] struct {
	keys []T
}
