package jdbc

import (
	"context"
	"fmt"
	"strings"

	"github.com/piyushsingariya/shift/safego"
	"github.com/piyushsingariya/shift/types"
)

type Offsetter[T types.Iterable] struct {
	query     string
	args      []any
	batchSize int
	offset    int
	err       chan error
	rows      chan T
	closed    bool
	ctx       context.Context

	exec        func(query string, args ...any) (T, error)
	withContext func(ctx context.Context, query string, args ...any) (T, error)
}

func NewOffsetter[T types.Iterable](baseQuery string, batchSize int,
	exec func(query string, args ...any) (T, error), args ...any) *Offsetter[T] {
	setter := &Offsetter[T]{
		query:     baseQuery,
		batchSize: batchSize,
		offset:    0,
		err:       make(chan error),
		rows:      make(chan T),
		exec:      exec,
		args:      args,
	}

	return setter
}

func WithContextOffsetter[T types.Iterable](ctx context.Context, baseQuery string, batchSize int,
	exec func(ctx context.Context, query string, args ...any) (T, error), args ...any) *Offsetter[T] {
	setter := &Offsetter[T]{
		query:       baseQuery,
		batchSize:   batchSize,
		offset:      0,
		err:         make(chan error),
		rows:        make(chan T),
		withContext: exec,
		args:        args,
	}

	return setter
}

func (o *Offsetter[T]) start() {
	for !o.closed {
		formattedQuery := fmt.Sprintf("%s OFFSET %d LIMIT %d", o.query, o.offset, o.batchSize)
		var rows T
		var err error
		if o.withContext != nil {
			rows, err = o.withContext(o.ctx, formattedQuery, o.args...)
		} else {
			rows, err = o.exec(formattedQuery, o.args...)
		}
		if err != nil {
			o.err <- err

			return
		}

		o.rows <- rows
	}
}

func (o *Offsetter[T]) Close() {
	o.closed = true
	safego.Close(o.err)
	safego.Close(o.rows)
}

func (o *Offsetter[T]) Capture(onCapture func(T) error) error {
	defer o.Close()

	if strings.HasSuffix(o.query, ";") {
		return fmt.Errorf("base query ends with ';': %s", o.query)
	}

	go o.start()

	for {
		select {
		case err := <-o.err:
			return err
		case rows := <-o.rows:
			length := 0
			for rows.Next() {
				err := onCapture(rows)
				if err != nil {
					return err
				}

				length++
			}

			err := rows.Err()
			if err != nil {
				return err
			}

			if length != o.batchSize {
				return nil
			}

			o.offset += length
		}
	}
}
