package models

import (
	"github.com/piyushsingariya/syndicate/jsonschema/schema"
	"github.com/piyushsingariya/syndicate/types"
)

// Stream is a dto for Airbyte catalog Stream object serialization
type Stream struct {
	Name                       string            `json:"name,omitempty"`
	Namespace                  string            `json:"namespace,omitempty"`
	JsonSchema                 *Schema           `json:"json_schema,omitempty"`
	SupportedSyncModes         []types.SyncMode  `json:"supported_sync_modes,omitempty"`
	SourceDefinedPrimaryKey    []string          `json:"source_defined_primary_key,omitempty"`
	SourceDefinedCursor        bool              `json:"source_defined_cursor"`
	DefaultCursorField         []string          `json:"default_cursor_field,omitempty"`
	AdditionalProperties       string            `json:"additional_properties,omitempty"`
	AdditionalPropertiesSchema schema.JSONSchema `json:"additional_properties_schema,omitempty"`
}

func NewAbstractStream(name, namespace string, syncModes []types.SyncMode) *Stream {
	return &Stream{
		Name:               name,
		Namespace:          namespace,
		SupportedSyncModes: syncModes,
	}
}
