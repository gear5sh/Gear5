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
		if stream.Stream == streamName {
			return stream.State
		}
	}

	return nil
}

type StreamState struct {
	Stream    string         `json:"stream"`
	Namespace string         `json:"namespace"`
	State     map[string]any `json:"state"`
}
