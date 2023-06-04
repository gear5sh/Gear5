package protocol

import (
	"github.com/piyushsingariya/syndicate/jsonschema/schema"
	"github.com/piyushsingariya/syndicate/models"
)

type Connector interface {
	Setup(config, state, catalog interface{}, batchSize int64) error
	Spec() (schema.JSONSchema, error)
	Check() error
	Discover() ([]*models.Stream, error)

	Catalog() *models.Catalog
	Type() string
}

type Driver interface {
	Connector
	Streams() ([]*models.Stream, error)
	Read(name string, channel chan<- models.RecordRow) error
}

type Adapter interface {
	Connector
	Write(channel <-chan models.RecordRow) error
	Create(streamName string) error
}
