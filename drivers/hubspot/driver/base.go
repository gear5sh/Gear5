package driver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	syndicatemodels "github.com/piyushsingariya/syndicate/models"
)

type BaseStream struct {
	*syndicatemodels.Stream
	entity string
	client *http.Client
	scopes []string
}

func (b *BaseStream) properties() error {
	if b.entity == "" {
		return fmt.Errorf("entity found to be empty")
	}
	req, err := http.NewRequest("GET", formatEndpoint(fmt.Sprintf("/properties/v2/%s/properties", b.entity)), nil)
	if err != nil {
		return err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	response := []map[string]interface{}{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return err
	}

	b.JSONSchema.Properties = make(map[string]*syndicatemodels.Property)

	for _, row := range response {
		b.JSONSchema.Properties[row["name"].(string)] = getFieldProps(row["type"].(string))
	}

	return nil
}

func (b *BaseStream) Read(channel <-chan syndicatemodels.Record) error {
	return fmt.Errorf("no implementation on base stream")
}
