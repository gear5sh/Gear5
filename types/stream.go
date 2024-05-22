package types

import (
	"fmt"

	"github.com/piyushsingariya/shift/jsonschema/schema"
	"github.com/piyushsingariya/shift/utils"
)

// Input/Processed object for Stream
type ConfiguredStream struct {
	Stream         *Stream      `json:"stream,omitempty"`
	SyncMode       SyncMode     `json:"sync_mode,omitempty"`    // Mode being used for syncing data
	CursorField    string       `json:"cursor_field,omitempty"` // Column being used as cursor; MUST NOT BE mutated
	ExcludeColumns []string     `json:"exclude_columns"`        // TODO: Implement excluding columns from fetching
	CursorValue    any          `json:"-"`                      // Cached initial state value
	batchSize      int64        `json:"-"`                      // Batch size for syncing data
	state          *StreamState `json:"-"`                      // in-memory state copy for individual stream

	// DestinationSyncMode string   `json:"destination_sync_mode,omitempty"`
}

// Output Stream Object for dsynk
type Stream struct {
	Name                       string            `json:"name,omitempty"`
	Namespace                  string            `json:"namespace,omitempty"`
	Schema                     *TypeSchema       `json:"json_schema,omitempty"`
	SupportedSyncModes         *Set[SyncMode]    `json:"supported_sync_modes,omitempty"`
	SourceDefinedPrimaryKey    *Set[string]      `json:"source_defined_primary_key,omitempty"`
	SourceDefinedCursor        bool              `json:"source_defined_cursor"`
	DefaultCursorFields        *Set[string]      `json:"default_cursor_fields,omitempty"`
	AdditionalProperties       string            `json:"additional_properties,omitempty"`
	AdditionalPropertiesSchema schema.JSONSchema `json:"additional_properties_schema,omitempty"`
}

func (s *ConfiguredStream) ID() string {
	return s.Stream.ID()
}

func (s *ConfiguredStream) Self() *ConfiguredStream {
	return s
}

func (s *ConfiguredStream) Name() string {
	return s.Stream.Name
}

func (s *ConfiguredStream) GetStream() *Stream {
	return s.Stream
}

func (s *ConfiguredStream) Namespace() string {
	return s.Stream.Namespace
}

func (s *ConfiguredStream) Schema() *TypeSchema {
	return s.Stream.Schema
}

func (s *ConfiguredStream) SupportedSyncModes() *Set[SyncMode] {
	return s.Stream.SupportedSyncModes
}

func (s *ConfiguredStream) GetSyncMode() SyncMode {
	return s.SyncMode
}

func (s *ConfiguredStream) Cursor() string {
	return s.CursorField
}

func (s *ConfiguredStream) InitialState() any {
	return s.CursorValue
}

func (s *ConfiguredStream) SetState(value any) {
	if s.state == nil {
		s.state = &StreamState{
			Stream:    s.Name(),
			Namespace: s.Namespace(),
			State: map[string]any{
				s.Cursor(): value,
			},
		}
		return
	}

	s.state.State[s.Cursor()] = value
}

func (s *ConfiguredStream) GetState() any {
	if s.state == nil || s.state.State == nil {
		return nil
	}
	return s.state.State[s.Cursor()]
}

func (s *ConfiguredStream) BatchSize() int64 {
	return s.batchSize
}

func (s *ConfiguredStream) SetBatchSize(size int64) {
	s.batchSize = size
}

// Returns empty and missing and error
// func (s *ConfiguredStream) SetupAndValidate(state *State) (StateError, error) {
// 	if !utils.ExistInArray(s.SupportedSyncModes(), s.SyncMode) {
// 		return "", fmt.Errorf("invalid sync mode[%s]; valid are %v", s.SyncMode, s.SupportedSyncModes())
// 	}

// 	if !utils.ExistInArray(s.Stream.DefaultCursorFields, s.CursorField) {
// 		return "", fmt.Errorf("invalid cursor field [%s]; valid are %v", s.SyncMode, s.SupportedSyncModes())
// 	}

// 	return s.setCursorValue(state), nil
// }

// Returns empty and missing
func (s *ConfiguredStream) SetupState(state *State) error {
	if !state.IsZero() {
		i, contains := utils.ArrayContains(state.Streams, func(elem *StreamState) bool {
			return elem.Namespace == s.Namespace() && elem.Stream == s.Name()
		})
		if contains {
			value, found := state.Streams[i].State[s.CursorField]
			if !found {
				return ErrStateCursorMissing
			}

			s.CursorValue = value

			return nil
		}

		return ErrStateMissing
	}

	return nil
}

// Validate Configured Stream with Source Stream
func (s *ConfiguredStream) Validate(source *Stream) error {
	if !utils.ExistInArray(source.SupportedSyncModes.Array(), s.SyncMode) {
		return fmt.Errorf("invalid sync mode[%s]; valid are %v", s.SyncMode, s.SupportedSyncModes().Array())
	}

	if !utils.ExistInArray(source.DefaultCursorFields.Array(), s.CursorField) {
		return fmt.Errorf("invalid cursor field [%s]; valid are %v", s.SyncMode, s.SupportedSyncModes())
	}

	if !source.SourceDefinedPrimaryKey.ProperSubsetOf(s.Stream.SourceDefinedPrimaryKey) {
		return fmt.Errorf("differnce found with primary keys: %v", source.SourceDefinedPrimaryKey.Difference(s.Stream.SourceDefinedPrimaryKey).Array())
	}

	return nil
}

func NewStream(name, namespace string) *Stream {
	return &Stream{
		Name:      name,
		Namespace: namespace,
	}
}

func (s *Stream) ID() string {
	return utils.StreamIdentifier(s.Name, s.Namespace)
}

func (s *Stream) WithSyncMode(modes ...SyncMode) *Stream {
	for _, mode := range modes {
		if !s.SupportedSyncModes.Exists(mode) {
			s.SupportedSyncModes.Insert(mode)
		}
	}

	return s
}

func (s *Stream) WithPrimaryKey(keys ...string) *Stream {
	for _, key := range keys {
		if !s.SourceDefinedPrimaryKey.Exists(key) {
			s.SourceDefinedPrimaryKey.Insert(key)
		}
	}

	return s
}

func (s *Stream) WithCursorField(columns ...string) *Stream {
	for _, column := range columns {
		if !s.SourceDefinedPrimaryKey.Exists(column) {
			s.SourceDefinedPrimaryKey.Insert(column)
		}
	}

	s.SourceDefinedCursor = true
	return s
}

// Add or Update Column in Stream Type Schema
func (s *Stream) UpsertField(column string, typ DataType, nullable bool) {
	if s.Schema == nil {
		s.Schema = &TypeSchema{
			Properties: map[string]*Property{},
		}
	}

	property := &Property{
		Type: []DataType{typ},
	}

	// if typ == TIMESTAMP {
	// 	property.Format = "date-time"
	// }

	if nullable {
		property.Type = append(property.Type, NULL)
	}

	s.Schema.Properties[column] = property
}

func (s *Stream) WithSchema(schema TypeSchema) *Stream {
	s.Schema = &schema
	return s
}

func GetWrappedCatalog(streams []*Stream) *Catalog {
	catalog := &Catalog{
		Streams: []*ConfiguredStream{},
	}

	for _, stream := range streams {
		catalog.Streams = append(catalog.Streams, &ConfiguredStream{
			Stream: stream,
		})
	}

	return catalog
}

func (s *Stream) Wrap() *ConfiguredStream {
	return &ConfiguredStream{
		Stream:   s,
		SyncMode: FULLREFRESH,
	}
}

func StreamsToMap(streams ...*Stream) map[string]*Stream {
	output := make(map[string]*Stream)
	for _, stream := range streams {
		output[stream.ID()] = stream
	}

	return output
}
