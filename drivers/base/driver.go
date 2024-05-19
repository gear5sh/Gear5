package base

import "github.com/piyushsingariya/shift/types"

type Driver struct {
	*types.State
}

func NewDriver(state *types.State) *Driver {
	return &Driver{
		State: state,
	}
}

// func (d *Driver) Catalog() *types.Catalog {
// 	return d.catalog
// }

// func (d *Driver) GetState() types.State {
// 	return d.State
// }
