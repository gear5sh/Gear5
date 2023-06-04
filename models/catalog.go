package models

import (
	"github.com/piyushsingariya/syndicate/types"
)

const (
	LogType              = "LOG"
	ConnectionStatusType = "CONNECTION_STATUS"
	StateType            = "STATE"
	RecordType           = "RECORD"
	CatalogType          = "CATALOG"
	SpecType             = "SPEC"
	ActionType           = "ACTION"
)

// Message is a dto for syndicate output row representation
type Message struct {
	Type             types.MessageType      `json:"type"`
	Log              *Log                   `json:"log,omitempty"`
	ConnectionStatus *StatusRow             `json:"connectionStatus,omitempty"`
	State            *State                 `json:"state,omitempty"`
	Record           *RecordRow             `json:"record,omitempty"`
	Catalog          *Catalog               `json:"catalog,omitempty"`
	Action           *ActionRow             `json:"action,omitempty"`
	Spec             map[string]interface{} `json:"spec,omitempty"`
}

type ActionRow struct {
	Type types.Action `json:"type"`
	// Add alter
	// add create
	// add drop
	// add truncate
}

// Log is a dto for airbyte logs serialization
type Log struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message,omitempty"`
}

// StatusRow is a dto for airbyte result status serialization
type StatusRow struct {
	Status  types.ConnectionStatus `json:"status,omitempty"`
	Message string                 `json:"message,omitempty"`
}

// State is a dto for airbyte state serialization
type State struct {
	Data map[string]interface{} `json:"data,omitempty"`
}

// RecordRow is a dto for airbyte record serialization
type RecordRow struct {
	Stream string                 `json:"stream,omitempty"`
	Data   map[string]interface{} `json:"data,omitempty"`
}

// ConfiguredCatalog is a dto for formatted airbyte catalog serialization
type Catalog struct {
	Streams []*WrappedStream `json:"streams,omitempty"`
}

// WrappedStream is a dto for formatted stream
type WrappedStream struct {
	SyncMode            string   `json:"sync_mode,omitempty"`
	DestinationSyncMode string   `json:"destination_sync_mode,omitempty"`
	CursorField         []string `json:"cursor_field,omitempty"`
	Stream              *Stream  `json:"stream,omitempty"`
}

// Schema is a dto for Airbyte catalog Schema object serialization
type Schema struct {
	Properties map[string]*Property `json:"properties,omitempty"`
}

// Property is a dto for catalog properties representation
type Property struct {
	//might be string or []string or nil
	Type       []types.DataType     `json:"type,omitempty"`
	Format     string               `json:"format,omitempty"`
	Properties map[string]*Property `json:"properties,omitempty"`
}
