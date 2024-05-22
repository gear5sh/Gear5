package types

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/piyushsingariya/shift/utils"
)

// Message is a dto for shift output row representation
type Message struct {
	Type             MessageType            `json:"type"`
	Log              *Log                   `json:"log,omitempty"`
	ConnectionStatus *StatusRow             `json:"connectionStatus,omitempty"`
	State            *State                 `json:"state,omitempty"`
	Record           *Record                `json:"record,omitempty"`
	Catalog          *Catalog               `json:"catalog,omitempty"`
	Action           *ActionRow             `json:"action,omitempty"`
	Spec             map[string]interface{} `json:"spec,omitempty"`
}

type ActionRow struct {
	// Type Action `json:"type"`
	// Add alter
	// add create
	// add drop
	// add truncate
}

// Log is a dto for airbyte logs serialization
type Log struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message,omitempty"`
}

// StatusRow is a dto for airbyte result status serialization
type StatusRow struct {
	Status  ConnectionStatus `json:"status,omitempty"`
	Message string           `json:"message,omitempty"`
}

// Record is a dto for airbyte record serialization
type Record struct {
	// close is used to stop iterating records
	Close     bool                   `json:"-"`
	Namespace string                 `json:"namespace,omitempty"`
	Stream    string                 `json:"stream,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
	EmittedAt time.Time              `json:"emitted_at,omitempty"`
}

// ConfiguredCatalog is a dto for formatted airbyte catalog serialization
type Catalog struct {
	Streams []*ConfiguredStream `json:"streams,omitempty"`
}

// Schema is a dto for Airbyte catalog Schema object serialization
type TypeSchema struct {
	Properties map[string]*Property `json:"properties,omitempty"`
}

func (t *TypeSchema) GetType(column string) (DataType, error) {
	p, found := t.Properties[column]
	if !found {
		return "", fmt.Errorf("column [%s] missing from type schema", column)
	}

	return p.DataType(), nil
}

func (t *TypeSchema) ToArrow() *arrow.Schema {
	fields := []arrow.Field{}

	for key, p := range t.Properties {
		fields = append(fields, arrow.Field{
			Name:     key,
			Type:     p.DataType().ToArrow(),
			Nullable: p.Nullable(),
		})
	}

	return arrow.NewSchema(fields, nil)
}

// Property is a dto for catalog properties representation
type Property struct {
	Type []DataType `json:"type,omitempty"`
	// TODO: Decide to keep in the Protocol Or Not
	// Format string     `json:"format,omitempty"`
}

func (p *Property) DataType() DataType {
	i, found := utils.ArrayContains(p.Type, func(elem DataType) bool {
		return elem != NULL
	})
	if !found {
		return NULL
	}

	return p.Type[i]
}

func (p *Property) Nullable() bool {
	_, found := utils.ArrayContains(p.Type, func(elem DataType) bool {
		return elem == NULL
	})

	return found
}
