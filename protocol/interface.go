package protocol

import (
	"github.com/piyushsingariya/shift/drivers/base"
	"github.com/piyushsingariya/shift/types"
)

type Connector interface {
	Setup(config any, base *base.Driver) error
	Spec() any
	Check() error

	Type() string
}

type Driver interface {
	Connector
	Discover() ([]*types.Stream, error)
	Read(stream Stream, channel chan<- types.Record) error
	BulkRead() bool
}

// Bulk Read Driver
type BulkDriver interface {
	GroupRead(channel chan<- types.Record, streams ...Stream) error
	GlobalState() any
	StateType() types.StateType
}

type Adapter interface {
	Connector
	Write(channel <-chan types.Record) error
	Create(streamName string) error
}

type Stream interface {
	ID() string
	Self() *types.ConfiguredStream
	Name() string
	Namespace() string
	Schema() *types.TypeSchema
	GetStream() *types.Stream
	GetSyncMode() types.SyncMode
	SupportedSyncModes() *types.Set[types.SyncMode]
	Cursor() string
	InitialState() any
	GetState() any
	SetState(value any)
	BatchSize() int64
	SetBatchSize(size int64)
	Validate(source *types.Stream) error
}
