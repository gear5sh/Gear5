package types

type Iterable interface {
	Next() bool
	Err() error
}
