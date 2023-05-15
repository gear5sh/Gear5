package models

import (
	"github.com/piyushsingariya/syndicate/constants"
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

// Row is a dto for airbyte output row representation
type Row struct {
	Type             string                 `json:"type"`
	Log              *LogRow                `json:"log,omitempty"`
	ConnectionStatus *StatusRow             `json:"connectionStatus,omitempty"`
	State            *StateRow              `json:"state,omitempty"`
	Record           *RecordRow             `json:"record,omitempty"`
	Catalog          *CatalogRow            `json:"catalog,omitempty"`
	Action           *ActionRow             `json:"action,omitempty"`
	Spec             map[string]interface{} `json:"spec,omitempty"`
}

type ActionRow struct {
	Type constants.Action `json:"type"`
	// Add alter
	// add create
	// add drop
	// add truncate
}

// LogRow is a dto for airbyte logs serialization
type LogRow struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message,omitempty"`
}

// StatusRow is a dto for airbyte result status serialization
type StatusRow struct {
	Status  string `json:"status,omitempty"`
	Message string `json:"message,omitempty"`
}

// StateRow is a dto for airbyte state serialization
type StateRow struct {
	Data map[string]interface{} `json:"data,omitempty"`
}

// RecordRow is a dto for airbyte record serialization
type RecordRow struct {
	Stream string                 `json:"stream,omitempty"`
	Data   map[string]interface{} `json:"data,omitempty"`
}

// Catalog is a dto for formatted airbyte catalog serialization
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

// CatalogRow is a dto for Airbyte discover output serialization
type CatalogRow struct {
	Streams []*Stream `json:"streams,omitempty"`
}

// Stream is a dto for Airbyte catalog Stream object serialization
type Stream struct {
	Name                    string     `json:"name,omitempty"`
	JsonSchema              *Schema    `json:"json_schema,omitempty"`
	SupportedSyncModes      []string   `json:"supported_sync_modes,omitempty"`
	SourceDefinedPrimaryKey [][]string `json:"source_defined_primary_key,omitempty"`
	SourceDefinedCursor     bool       `json:"source_defined_cursor"`
	DefaultCursorField      []string   `json:"default_cursor_field,omitempty"`

	Namespace string   `json:"namespace,omitempty"`
	SortKey   []string `json:"sort_key,omitempty"`

	SyncMode            string   `json:"-" yaml:"-"` //without serialization
	SelectedCursorField []string `json:"-" yaml:"-"` //without serialization
}

// Schema is a dto for Airbyte catalog Schema object serialization
type Schema struct {
	Properties map[string]*Property `json:"properties,omitempty"`
}

// Property is a dto for catalog properties representation
type Property struct {
	//might be string or []string or nil
	Type       []constants.DataType `json:"type,omitempty"`
	Format     string               `json:"format,omitempty"`
	Properties map[string]*Property `json:"properties,omitempty"`
}
