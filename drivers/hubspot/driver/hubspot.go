package driver

import (
	"github.com/piyushsingariya/syndicate/drivers/hubspot/models"
	"github.com/piyushsingariya/syndicate/jsonschema"
	"github.com/piyushsingariya/syndicate/jsonschema/schema"
	syndicatemodels "github.com/piyushsingariya/syndicate/models"
)

type Hubspot struct {
	config  *models.Config
	catalog *syndicatemodels.Catalog
}

func (h *Hubspot) Setup(config, catalog, state interface{}, batchSize int64) error {
	return nil
}
func (h *Hubspot) Spec() (schema.JSONSchema, error) {
	return jsonschema.Reflect(models.Config{})
}

func (h *Hubspot) Check() error {
	return nil
}
func (h *Hubspot) Discover() ([]*syndicatemodels.Stream, bool, error) {
	return nil, false, nil
}

func (h *Hubspot) Catalog() *syndicatemodels.Catalog {
	return h.catalog
}
func (h *Hubspot) Type() string {
	return "Hubspot"
}

func (h *Hubspot) Streams() ([]*syndicatemodels.Stream, error) {
	return nil, nil
}

func (h *Hubspot) Read(stream *syndicatemodels.Stream, channel chan<- syndicatemodels.Record) error {
	return nil
}
