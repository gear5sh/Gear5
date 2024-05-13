package types

import (
	"fmt"

	"github.com/piyushsingariya/shift/jsonschema/schema"
	"github.com/piyushsingariya/shift/utils"
)

// Input/Processed object for Stream
type WrappedStream struct {
	Stream      *Stream      `json:"stream,omitempty"`
	SyncMode    SyncMode     `json:"sync_mode,omitempty"`    // Mode being used for syncing data
	CursorField string       `json:"cursor_field,omitempty"` // Column being used as cursor; MUST NOT BE mutated
	CursorValue any          `json:"-"`                      // Cached initial state value
	batchSize   int64        `json:"-"`                      // Batch size for syncing data
	state       *StreamState `json:"-"`                      // in-memory state copy for individual stream

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

func (s *WrappedStream) ID() string {
	return fmt.Sprintf("%s[%s]", s.Name(), s.Namespace())
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

func (s *WrappedStream) Cursor() string {
	return s.CursorField
}

func (s *WrappedStream) InitialState() any {
	return s.CursorValue
}

func (s *WrappedStream) SetState(value any) {
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

func (s *WrappedStream) GetState() any {
	if s.state == nil || s.state.State == nil {
		return nil
	}
	return s.state.State[s.Cursor()]
}

func (s *WrappedStream) BatchSize() int64 {
	return s.batchSize
}

func (s *WrappedStream) SetBatchSize(size int64) {
	s.batchSize = size
}

// Returns empty and missing and error
func (s *WrappedStream) SetupAndValidate(state State) (StateError, error) {
	if !utils.ExistInArray(s.SupportedSyncModes(), s.SyncMode) {
		return "", fmt.Errorf("invalid sync mode[%s]; valid are %v", s.SyncMode, s.SupportedSyncModes())
	}

	if !utils.ExistInArray(s.Stream.DefaultCursorFields, s.CursorField) {
		return "", fmt.Errorf("invalid cursor field [%s]; valid are %v", s.SyncMode, s.SupportedSyncModes())
	}

	return s.setCursorValue(state), nil
}

// Returns empty and missing
func (s *WrappedStream) setCursorValue(state State) StateError {
	if !state.IsZero() {
		i, contains := utils.ArrayContains(state, func(elem *StreamState) bool {
			return elem.Namespace == s.Namespace() && elem.Stream == s.Name()
		})
		if contains {
			value, found := state[i].State[s.CursorField]
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

func NewStream(name, namespace string) *WrappedStream {
	return &WrappedStream{
		Stream: &Stream{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func (s *WrappedStream) Self() *WrappedStream {
	return s
}

func (s *WrappedStream) WithSyncModes(modes ...SyncMode) *WrappedStream {
	s.Stream.SupportedSyncModes = modes
	return s
}

func (s *WrappedStream) WithPrimaryKeys(keys ...string) *WrappedStream {
	s.Stream.SourceDefinedPrimaryKey = keys
	return s
}

func (s *WrappedStream) WithCursorFields(columns ...string) *WrappedStream {
	s.Stream.DefaultCursorFields = columns
	s.Stream.SourceDefinedCursor = true
	return s
}

func (s *WrappedStream) WithJSONSchema(schema Schema) *WrappedStream {
	s.Stream.JSONSchema = &schema
	return s
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

func WrapStream(stream *Stream) *WrappedStream {
	return &WrappedStream{
		Stream: stream,
	}
}
