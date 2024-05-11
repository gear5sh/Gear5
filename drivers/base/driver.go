package base

import (
	"github.com/piyushsingariya/shift/types"
)

type Driver struct {
	types.State

	catalog   *types.Catalog
	batchSize uint64
}

func NewDriver(catalog *types.Catalog, state types.State, batchSize uint64) *Driver {
	return &Driver{
		catalog:   catalog,
		State:     state,
		batchSize: batchSize,
	}
}

func (d *Driver) BatchSize() uint64 {
	return d.batchSize
}

func (d *Driver) SetBatchSize(size uint64) {
	d.batchSize = size
}

func (d *Driver) Catalog() *types.Catalog {
	return d.catalog
}

func (d *Driver) GetState() types.State {
	return d.State
}
