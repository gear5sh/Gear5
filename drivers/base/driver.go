package base

import (
	"github.com/piyushsingariya/shift/types"
)

type Driver struct {
	SourceStreams map[string]*types.Stream // locally cached streams; It contains all streams
	GroupRead     bool                     // Used in CDC mode
}

func NewBase() *Driver {
	return &Driver{
		SourceStreams: make(map[string]*types.Stream),
	}
}

func (d *Driver) BulkRead() bool {
	return d.GroupRead
}
