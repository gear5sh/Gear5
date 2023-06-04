package driver

import (
	"fmt"
	"net/http"

	"github.com/piyushsingariya/syndicate/drivers/hubspot/models"
	"github.com/piyushsingariya/syndicate/jsonschema"
	"github.com/piyushsingariya/syndicate/jsonschema/schema"
	syndicatemodels "github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/utils"
)

type Hubspot struct {
	client  *http.Client
	config  *models.Config
	catalog *syndicatemodels.Catalog
}

func (h *Hubspot) Setup(config, catalog, state interface{}, batchSize int64) error {
	conf := &models.Config{}
	if err := utils.Unmarshal(config, conf); err != nil {
		return err
	}

	if err := conf.ValidateAndPopulateDefaults(); err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	if catalog != nil {
		cat := &syndicatemodels.Catalog{}
		if err := utils.Unmarshal(catalog, cat); err != nil {
			return err
		}

		h.catalog = cat
	}

	client, err := newClient(conf)
	if err != nil {
		return err
	}

	h.client = client
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
