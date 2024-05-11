package types

// State is a dto for airbyte state serialization
type State []*StreamState

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

func (s *State) Update(streamName, namespace string, field string, value any) {
	found := false
	for _, stream := range *s {
		if stream.Stream == streamName && stream.Namespace == namespace {
			stream.State[field] = value
			found = true
		}
	}

	if !found {
		s.Add(streamName, namespace, field, value)
	}
}

func (s *State) IsZero() bool {
	return len(*s) == 0
}

type StreamState struct {
	Stream    string         `json:"stream"`
	Namespace string         `json:"namespace"`
	State     map[string]any `mapstructure:"state" json:"state"`
}
