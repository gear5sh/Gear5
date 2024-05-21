package types

import "sync/atomic"

type BatchSize struct {
	maximum int
	size    atomic.Int64
}

func NewBatchSize(maximum int) *BatchSize {
	return &BatchSize{
		maximum: maximum,
		size:    atomic.Int64{},
	}
}

func (b *BatchSize) Set(size int) {
	b.size.Store(int64(size))
}

func (b *BatchSize) Size() int {
	return int(b.size.Load())
}
