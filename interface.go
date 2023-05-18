package syndicate

import "github.com/piyushsingariya/syndicate/models"

type Connector interface {
	Setup(config, state, catalog interface{}, batchSize int) error
	Check() error
	Discover() ([]*models.Stream, error)

	Type() string
	Schema() string
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
