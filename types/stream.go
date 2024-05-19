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
	JSONSchema                 *Schema           `json:"json_schema,omitempty"`
	SupportedSyncModes         []SyncMode        `json:"supported_sync_modes,omitempty"`
	SourceDefinedPrimaryKey    []string          `json:"source_defined_primary_key,omitempty"`
	SourceDefinedCursor        bool              `json:"source_defined_cursor"`
	DefaultCursorFields        []string          `json:"default_cursor_fields,omitempty"`
	AdditionalProperties       string            `json:"additional_properties,omitempty"`
	AdditionalPropertiesSchema schema.JSONSchema `json:"additional_properties_schema,omitempty"`
}

func (s *ConfiguredStream) ID() string {
	if s.Namespace() != "" {
		return fmt.Sprintf("%s.%s", s.Namespace(), s.Name())
	}

	return s.Name()
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

func (s *ConfiguredStream) JSONSchema() *Schema {
	return s.Stream.JSONSchema
}

func (s *ConfiguredStream) SupportedSyncModes() []SyncMode {
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
func (s *ConfiguredStream) SetupAndValidate(state *State) (StateError, error) {
	if !utils.ExistInArray(s.SupportedSyncModes(), s.SyncMode) {
		return "", fmt.Errorf("invalid sync mode[%s]; valid are %v", s.SyncMode, s.SupportedSyncModes())
	}

	if !utils.ExistInArray(s.Stream.DefaultCursorFields, s.CursorField) {
		return "", fmt.Errorf("invalid cursor field [%s]; valid are %v", s.SyncMode, s.SupportedSyncModes())
	}

	return s.setCursorValue(state), nil
}

// Returns empty and missing
func (s *ConfiguredStream) setCursorValue(state *State) StateError {
	if !state.IsZero() {
		i, contains := utils.ArrayContains(state.Streams, func(elem *StreamState) bool {
			return elem.Namespace == s.Namespace() && elem.Stream == s.Name()
		})
		if contains {
			value, found := state.Streams[i].State[s.CursorField]
			if !found {
				return StateCursorMissing
			}

			s.CursorValue = value

			return StateValid
		}

		return StateMissing
	}

	return StateValid
}

func NewStream(name, namespace string) *Stream {
	return &Stream{
		Name:      name,
		Namespace: namespace,
	}
}

func (s *Stream) WithSyncModes(modes ...SyncMode) *Stream {
	s.SupportedSyncModes = modes
	return s
}

func (s *Stream) WithPrimaryKeys(keys ...string) *Stream {
	s.SourceDefinedPrimaryKey = keys
	return s
}

func (s *Stream) WithCursorFields(columns ...string) *Stream {
	s.DefaultCursorFields = columns
	s.SourceDefinedCursor = true
	return s
}

func (s *Stream) WithJSONSchema(schema Schema) *Stream {
	s.JSONSchema = &schema
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
