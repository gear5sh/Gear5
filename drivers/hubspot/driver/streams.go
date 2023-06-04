package driver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	syndicatemodels "github.com/piyushsingariya/syndicate/models"
)

type Stream struct {
	*syndicatemodels.Stream
	entity string
	client *http.Client
}

func (s *Stream) properties() (map[string]interface{}, error) {
	if s.entity == "" {
		return nil, fmt.Errorf("entity found to be empty")
	}
	// data, response = self._api.get(f"/properties/v2/{self.entity}/properties")
	//     for row in data:
	//         props[row["name"]] = self._get_field_props(row["type"])
	req, err := http.NewRequest("GET", fmt.Sprintf("/properties/v2/%s/properties", s.entity), nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	response := []map[string]interface{}{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	s.JSONSchema.Properties = make(map[string]*syndicatemodels.Property)

	for _, row := range response {
		s.JSONSchema.Properties[row["name"].(string)] = getFieldProps(row["type"])
	}

}
