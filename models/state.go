package models

// State is a dto for airbyte state serialization
type State []*StreamState

func (s *State) Add(stream, namespace string, state map[string]any) {
	*s = append(*s, &StreamState{
		Stream:    stream,
		Namespace: namespace,
		State:     state,
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

func (s *State) Update(streamName, namespace string, state map[string]interface{}) {
	found := false
	for _, stream := range *s {
		if stream.Stream == streamName && stream.Namespace == namespace {
			stream.State = state
			found = true
		}
	}

	if !found {
		s.Add(streamName, namespace, state)
	}
}

func (s *State) Len() int {
	return len(*s)
}

type StreamState struct {
	Stream    string         `json:"stream"`
	Namespace string         `json:"namespace"`
	State     map[string]any `mapstructure:"state" json:"state"`
}
