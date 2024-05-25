package types

import "errors"

type StateType string

const (
	GlobalType StateType = "GLOBAL"
	StreamType StateType = "STREAM"
	MixedType  StateType = "MIXED"
)

// State is a dto for airbyte state serialization
type State struct {
	Type    StateType      `json:"type"`
	Global  any            `json:"global,omitempty"`
	Streams []*StreamState `json:"streams,omitempty"`
}

var (
	ErrStateMissing       = errors.New("stream missing from state")
	ErrStateCursorMissing = errors.New("cursor field missing from state")
)

func (s *State) SetType(typ StateType) {
	s.Type = typ
}

// func (s *State) Add(stream, namespace string, field string, value any) {
// 	s.Streams = append(s.Streams, &StreamState{
// 		Stream:    stream,
// 		Namespace: namespace,
// 		State: map[string]any{
// 			field: value,
// 		},
// 	})
// }

// func (s *State) Get(streamName, namespace string) map[string]any {
// 	for _, stream := range s.Streams {
// 		if stream.Stream == streamName && stream.Namespace == namespace {
// 			return stream.State
// 		}
// 	}

// 	return nil
// }

func (s *State) IsZero() bool {
	return s.Global == nil && len(s.Streams) == 0
}

type StreamState struct {
	Stream    string         `json:"stream"`
	Namespace string         `json:"namespace"`
	State     map[string]any `mapstructure:"state" json:"state"`
}
