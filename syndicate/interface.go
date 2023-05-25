package syndicate

import "github.com/piyushsingariya/syndicate/models"

type Connector interface {
	Setup(config, state, catalog interface{}, batchSize int) error
	Spec() (interface{}, error)
	Check() error
	Discover() ([]*models.Stream, error)

	Type() string
}

type Driver interface {
	Connector
	Read()
}

type Adapter interface {
	Connector
	Write()
	Create(streamName string) error
}

type Stream interface {
	GetPrimaryKey() []string
	GetStreamConfiguration() interface{}
}
