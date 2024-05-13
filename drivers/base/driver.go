package base

import "github.com/piyushsingariya/shift/types"

type Driver struct {
	// types.State

	// catalog *types.Catalog
}

func NewDriver(catalog *types.Catalog, state types.State) *Driver {
	return &Driver{
		// catalog: catalog,
		// State:   state,
	}
}

// func (d *Driver) Catalog() *types.Catalog {
// 	return d.catalog
// }

// func (d *Driver) GetState() types.State {
// 	return d.State
// }
