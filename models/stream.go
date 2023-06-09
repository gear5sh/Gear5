package models

import (
	"fmt"

	"github.com/piyushsingariya/syndicate/jsonschema/schema"
	"github.com/piyushsingariya/syndicate/types"
)

// WrappedStream is a dto for formatted stream
type WrappedStream struct {
	SyncMode            types.SyncMode `json:"sync_mode,omitempty"`
	DestinationSyncMode string         `json:"destination_sync_mode,omitempty"`
	CursorField         []string       `json:"cursor_field,omitempty"`
	Stream              *Stream        `json:"stream,omitempty"`
}

// Stream is a dto for Airbyte catalog Stream object serialization
type Stream struct {
	Name                       string            `json:"name,omitempty"`
	Namespace                  string            `json:"namespace,omitempty"`
	JSONSchema                 *Schema           `json:"json_schema,omitempty"`
	SupportedSyncModes         []types.SyncMode  `json:"supported_sync_modes,omitempty"`
	SourceDefinedPrimaryKey    []string          `json:"source_defined_primary_key,omitempty"`
	SourceDefinedCursor        bool              `json:"source_defined_cursor"`
	DefaultCursorField         []string          `json:"default_cursor_field,omitempty"`
	AdditionalProperties       string            `json:"additional_properties,omitempty"`
	AdditionalPropertiesSchema schema.JSONSchema `json:"additional_properties_schema,omitempty"`
}

func (s *WrappedStream) Read(channel <-chan Record) error {
	return fmt.Errorf("abstract method not implemented")
}

func (s *WrappedStream) Name() string {
	return s.Stream.Name
}

func (s *WrappedStream) Namespace() string {
	return s.Stream.Namespace
}

func (s *WrappedStream) JSONSchema() *Schema {
	return s.Stream.JSONSchema
}

func (s *WrappedStream) SupportedSyncModes() []types.SyncMode {
	return s.Stream.SupportedSyncModes
}

func (s *WrappedStream) GetSyncMode() types.SyncMode {
	return s.SyncMode
}

func GetWrappedCatalog(streams []*Stream) *Catalog {
	catalog := &Catalog{
		Streams: []*WrappedStream{},
	}

	for _, stream := range streams {
		catalog.Streams = append(catalog.Streams, &WrappedStream{
			Stream: stream,
		})
	}

	return catalog
}
