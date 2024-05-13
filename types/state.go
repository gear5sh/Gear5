package types

// State is a dto for airbyte state serialization
type State []*StreamState
type StateError string

const (
	StateValid         StateError = "valid"
	StateMissing       StateError = "stream missing from state"
	StateCursorMissing StateError = "cursor field missing from state"
)

func (s *State) Add(stream, namespace string, field string, value any) {
	*s = append(*s, &StreamState{
		Stream:    stream,
		Namespace: namespace,
		State: map[string]any{
			field: value,
		},
	})
}

func (s *State) Get(streamName, namespace string) map[string]any {
	for _, stream := range *s {
		if stream.Stream == streamName && stream.Namespace == namespace {
			return stream.State
		}
	}

	return nil
}

func (s *State) IsZero() bool {
	return len(*s) == 0
}

type StreamState struct {
	Stream    string         `json:"stream"`
	Namespace string         `json:"namespace"`
	State     map[string]any `mapstructure:"state" json:"state"`
}
