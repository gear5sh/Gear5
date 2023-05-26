package protocol

import (
	"github.com/piyushsingariya/syndicate/jsonschema/schema"
	"github.com/piyushsingariya/syndicate/models"
)

type Connector interface {
	Setup(config, state, catalog interface{}, batchSize int) error
	Spec() (schema.JSONSchema, error)
	Check() error
	Discover() ([]*models.Stream, error)

	Type() string
}

type Driver interface {
	Connector
	Read(channel chan<- models.RecordRow) error
}

type Adapter interface {
	Connector
	Write(channel <-chan models.RecordRow) error
	Create(streamName string) error
}

type Stream interface {
	GetPrimaryKey() []string
	GetStreamConfiguration() interface{}
}
