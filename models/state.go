package models

// State is a dto for airbyte state serialization
type State []StreamState

type StreamState struct {
	Stream    string `json:"stream"`
	Namespace string `json:"namespace"`
	State     any    `json:"state"`
}
