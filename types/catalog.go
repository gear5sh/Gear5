package types

import (
	"time"
)

// Message is a dto for shift output row representation
type Message struct {
	Type             MessageType            `json:"type"`
	Log              *Log                   `json:"log,omitempty"`
	ConnectionStatus *StatusRow             `json:"connectionStatus,omitempty"`
	State            *State                 `json:"state,omitempty"`
	Record           *Record                `json:"record,omitempty"`
	Catalog          *Catalog               `json:"catalog,omitempty"`
	Action           *ActionRow             `json:"action,omitempty"`
	Spec             map[string]interface{} `json:"spec,omitempty"`
}

type ActionRow struct {
	Type Action `json:"type"`
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
	Status  ConnectionStatus `json:"status,omitempty"`
	Message string           `json:"message,omitempty"`
}

// Record is a dto for airbyte record serialization
type Record struct {
	// close is used to stop iterating records
	Close     bool                   `json:"-"`
	Namespace string                 `json:"namespace,omitempty"`
	Stream    string                 `json:"stream,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
	EmittedAt time.Time              `json:"emitted_at,omitempty"`
}

// ConfiguredCatalog is a dto for formatted airbyte catalog serialization
type Catalog struct {
	Streams []*WrappedStream `json:"streams,omitempty"`
}

// Schema is a dto for Airbyte catalog Schema object serialization
type Schema struct {
	Properties map[string]*Property `json:"properties,omitempty"`
}

// Property is a dto for catalog properties representation
type Property struct {
	Type       []DataType           `json:"type,omitempty"`
	Properties map[string]*Property `json:"properties,omitempty"`
}
