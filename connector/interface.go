package connector

type Connector interface {
	Check()
	Discover()
	Create(streamName string) error

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
}
