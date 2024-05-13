package types

import (
	"github.com/piyushsingariya/shift/jsonschema/schema"
	"github.com/piyushsingariya/shift/utils"
)

// Input/Processed object for Stream
type WrappedStream struct {
	Stream      *Stream  `json:"stream,omitempty"`
	SyncMode    SyncMode `json:"sync_mode,omitempty"`
	CursorField string   `json:"cursor_field,omitempty"`
	CursorValue any      `json:"-"`
	// DestinationSyncMode string   `json:"destination_sync_mode,omitempty"`
}

// Output Stream Object for dsynk
type Stream struct {
	Name                       string            `json:"name,omitempty"`
	Namespace                  string            `json:"namespace,omitempty"`
	JSONSchema                 *Schema           `json:"json_schema,omitempty"`
	SupportedSyncModes         []SyncMode        `json:"supported_sync_modes,omitempty"`
	SourceDefinedPrimaryKey    []string          `json:"source_defined_primary_key,omitempty"`
	SourceDefinedCursor        bool              `json:"source_defined_cursor"`
	DefaultCursorFields        []string          `json:"default_cursor_fields,omitempty"`
	AdditionalProperties       string            `json:"additional_properties,omitempty"`
	AdditionalPropertiesSchema schema.JSONSchema `json:"additional_properties_schema,omitempty"`
}

func (s *WrappedStream) Name() string {
	return s.Stream.Name
}

func (s *WrappedStream) GetStream() *Stream {
	return s.Stream
}

func (s *WrappedStream) Namespace() string {
	return s.Stream.Namespace
}

func (s *WrappedStream) JSONSchema() *Schema {
	return s.Stream.JSONSchema
}

func (s *WrappedStream) SupportedSyncModes() []SyncMode {
	return s.Stream.SupportedSyncModes
}

func (s *WrappedStream) GetSyncMode() SyncMode {
	return s.SyncMode
}

func (s *WrappedStream) GetCursorField() string {
	return s.CursorField
}

func (s *WrappedStream) GetCursorValue() any {
	return s.CursorValue
}

// Returns empty and missing
func (s *WrappedStream) SetCursorValue(state State) (bool, bool) {
	if !state.IsZero() {
		i, contains := utils.ArrayContains(state, func(elem *StreamState) bool {
			return elem.Namespace == s.Namespace() && elem.Stream == s.Name()
		})
		if contains {
			value, found := state[i].State[s.CursorField]
			if !found {
				return true, false
			}

			s.CursorValue = value

			return false, false
		}

		return false, true
	}

	return false, false
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
