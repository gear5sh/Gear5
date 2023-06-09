package driver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/piyushsingariya/syndicate/drivers/hubspot/models"
	"github.com/piyushsingariya/syndicate/jsonschema"
	"github.com/piyushsingariya/syndicate/jsonschema/schema"
	"github.com/piyushsingariya/syndicate/logger"
	syndicatemodels "github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/protocol"
	"github.com/piyushsingariya/syndicate/utils"
)

type Hubspot struct {
	client      *http.Client
	accessToken string
	config      *models.Config
	catalog     *syndicatemodels.Catalog

	allStreams []protocol.Stream
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

	client, accessToken, err := newClient(conf)
	if err != nil {
		return err
	}

	h.client = client
	h.accessToken = accessToken
	h.setupAllStreams()

	return nil
}

func (h *Hubspot) Spec() (schema.JSONSchema, error) {
	return jsonschema.Reflect(models.Config{})
}

func (h *Hubspot) Check() error {
	return nil
}
func (h *Hubspot) Discover() ([]*syndicatemodels.Stream, error) {
	return nil, nil
}

func (h *Hubspot) Catalog() *syndicatemodels.Catalog {
	return h.catalog
}
func (h *Hubspot) Type() string {
	return "Hubspot"
}

func (h *Hubspot) Streams() ([]*syndicatemodels.Stream, error) {
	scopes, err := h.getGrantedScopes()
	if err != nil {
		return nil, err
	}

	logger.Infof("The following scopes are granted: %v", scopes)
	return nil, nil
}

func (h *Hubspot) Read(stream protocol.Stream, channel chan<- syndicatemodels.Record) error {
	return nil
}

func (h *Hubspot) getGrantedScopes() ([]string, error) {
	req, err := http.NewRequest("GET", formatEndpoint(fmt.Sprintf("oauth/v1/access-tokens/%s", h.accessToken)), nil)
	if err != nil {
		return nil, err
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	response := map[string]any{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	return response["scopes"].([]string), nil
}

func (h *Hubspot) setupAllStreams() {
	h.allStreams = append(h.allStreams)
}
