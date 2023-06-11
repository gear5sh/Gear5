package models

// State is a dto for airbyte state serialization
type State struct {
	Data map[string]interface{} `json:"data,omitempty"`
}
