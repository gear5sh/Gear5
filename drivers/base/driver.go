package base

import (
	"github.com/piyushsingariya/shift/types"
)

type Driver struct {
	*types.State
	SourceStreams map[string]*types.Stream // locally cached streams; It contains all streams
	GroupRead     bool                     // Used in CDC mode
}

func NewDriver(state *types.State) *Driver {
	return &Driver{
		State:         state,
		SourceStreams: make(map[string]*types.Stream),
	}
}

func (d *Driver) GroupReadMode() bool {
	return d.GroupRead
}

// func (d *Driver) Catalog() *types.Catalog {
// 	return d.catalog
// }

// func (d *Driver) GetState() types.State {
// 	return d.State
// }
