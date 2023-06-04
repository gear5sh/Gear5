package protocol

import (
	"github.com/piyushsingariya/syndicate/jsonschema/schema"
	"github.com/piyushsingariya/syndicate/models"
)

type Connector interface {
	Setup(config, catalog, state interface{}, batchSize int64) error
	Spec() (schema.JSONSchema, error)
	Check() error
	Discover() ([]*models.Stream, bool, error)

	Catalog() *models.Catalog
	Type() string
}

type Driver interface {
	Connector
	Streams() ([]*models.Stream, error)
	Read(stream *models.Stream, channel chan<- models.Record) error
}

type Adapter interface {
	Connector
	Write(channel <-chan models.Record) error
	Create(streamName string) error
}
