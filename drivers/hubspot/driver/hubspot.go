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
	"github.com/piyushsingariya/syndicate/types"
	"github.com/piyushsingariya/syndicate/typing"
	"github.com/piyushsingariya/syndicate/utils"
)

type Hubspot struct {
	batchSize   int64
	allStreams  map[string]HubspotStream
	client      *http.Client
	accessToken string
	config      *models.Config
	catalog     *syndicatemodels.Catalog
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

	h.config = conf
	h.batchSize = batchSize
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
	streams := []*syndicatemodels.Stream{}

	for streamName, stream := range h.allStreams {
		objects := []types.RecordData{}
		channel := make(chan syndicatemodels.Record)
		count := int64(0)
		go func() {
			err := h.readForDiscover(stream, channel)
			if err != nil {
				logger.Fatalf("Error occurred while reading records from [%s]: %s", streamName, err)
			}
		}()

		for record := range channel {
			count++
			objects = append(objects, record.Data)
			if count >= h.batchSize {
				close(channel)
			}
		}

		properties, err := typing.Resolve(objects...)
		if err != nil {
			return nil, err
		}

		streams = append(streams, &syndicatemodels.Stream{
			Name: streamName,
			JSONSchema: &syndicatemodels.Schema{
				Properties: properties,
			},
			SupportedSyncModes:      stream.Modes(),
			SourceDefinedPrimaryKey: stream.PrimaryKey(),
		})

	}

	return streams, nil
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

func (h *Hubspot) readForDiscover(stream HubspotStream, channel chan<- syndicatemodels.Record) error {
	return stream.readRecords(channel)
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

func (h *Hubspot) register(streamName string, stream HubspotStream) {
	h.allStreams[streamName] = stream
}

func (h *Hubspot) setupAllStreams() {
	// initialization
	h.allStreams = map[string]HubspotStream{}

	h.register("contacts",
		newCRMSearchStream(
			*newIncrementalStream(
				*newStream("contacts", "contacts", "id", "lastmodifieddate", []string{"crm.objects.contacts.read"},
					h.client, h.config.StartDate),
				"updatedAt"),
			[]string{"contacts", "companies"},
		))
}
