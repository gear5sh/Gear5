package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/piyushsingariya/syndicate/logger"
	syndicatemodels "github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/utils"
)

type Stream struct {
	*syndicatemodels.WrappedStream
	entity            string
	updatedAtField    string
	createdAtField    string
	lastModifiedField string
	moreKey           string
	dataField         string
	pageFilter        string
	pageField         string
	limitField        string
	url               string
	limit             int
	offset            int
	batchSize         int

	primaryKey []string
	groupByKey string

	denormalizeRecords bool

	client           *http.Client
	grantedScopes    []string
	scopes           []string
	propertiesScopes []string

	startDate time.Time
}

func newStream(name, namespace, entity, groupByKey, lastModifiedKey string, scopes []string, client *http.Client, startDate time.Time) *Stream {
	return &Stream{
		WrappedStream: &syndicatemodels.WrappedStream{
			Stream: &syndicatemodels.Stream{
				Name:       name,
				Namespace:  namespace,
				JSONSchema: &syndicatemodels.Schema{},
			},
		},
		entity:            entity,
		groupByKey:        groupByKey,
		lastModifiedField: lastModifiedKey,
		scopes:            scopes,
		client:            client,
		startDate:         startDate,
	}
}

func (s *Stream) path() string {
	return s.url
}

func (s *Stream) properties() (map[string]*syndicatemodels.Property, error) {
	if s.entity == "" {
		return nil, fmt.Errorf("entity found to be empty")
	}

	if !s.propertiesScopeIsGranted() {
		return nil,
			fmt.Errorf("Check your API key has the following permissions granted: %v to be able to fetch all properties available.", s.propertiesScopes)
	}

	req, err := http.NewRequest("GET", formatEndpoint(fmt.Sprintf("/properties/v2/%s/properties", s.entity)), nil)
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

	response := []map[string]any{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	properties := make(map[string]*syndicatemodels.Property)

	for _, row := range response {
		properties[row["name"].(string)] = getFieldProps(row["type"].(string))
	}

	s.Stream.JSONSchema.Properties = properties

	return properties, nil
}

func (s *Stream) propertiesScopeIsGranted() bool {
	return utils.IsSubset(s.grantedScopes, s.propertiesScopes)
}

func (s *Stream) Read(channel <-chan syndicatemodels.Record) error {
	return fmt.Errorf("no implementation on base stream")
}

func (s *Stream) getStream() (*syndicatemodels.Stream, error) {
	if s.Stream.JSONSchema != nil {
		return s.Stream, nil
	}

	_, err := s.properties()
	if err != nil {
		return nil, err
	}

	return s.Stream, nil
}

func (s *Stream) setStream(stream *syndicatemodels.Stream) {
	s.Stream = stream
}

func (s *Stream) ScopeIsGranted(grantedScopes []string) bool {
	s.grantedScopes = utils.Set(grantedScopes)
	return utils.IsSubset(grantedScopes, s.scopes)
}

func (s *Stream) backoffTime(response *http.Response) float64 {
	if response.StatusCode == http.StatusTooManyRequests {
		parsed, err := strconv.ParseFloat(response.Header.Get("Retry-After"), 64)
		if err != nil {
			logger.Errorf("failed to parse retry-after: %s", err)
			return 0
		}

		return parsed
	}

	return 0
}

func (s *Stream) nextPageToken(response any) (map[string]any, error) {
	if resp, ok := response.(map[string]any); ok {
		if value, found := resp["paging"]; found {
			if paging, ok := value.(map[string]any); ok {
				if value, found := paging["next"]; found {
					if next, ok := value.(map[string]any); ok {
						return map[string]any{"after": next["after"]}, nil
					}
				}
			}
		} else {
			if s.moreKey != "" {
				if _, found := resp[s.moreKey]; !found {
					return nil, fmt.Errorf("key[%s] not found in response", s.moreKey)
				}
				if value, found := resp[s.pageField]; found {
					return map[string]any{s.pageFilter: value}, nil
				}
			}
			if _, found := resp[s.pageField]; found {
				if resp[s.pageFilter].(int)+s.limit < resp["total"].(int) {
					return map[string]any{s.pageFilter: resp[s.pageFilter].(int) + s.limit}, nil
				}
			}
		}
	} else if resp, ok := response.([]any); ok {
		if len(resp) >= s.limit {
			s.offset += s.limit
			return map[string]any{s.pageFilter: s.offset}, nil
		}
	}

	return nil, fmt.Errorf("failed to get next page token")
}

func (s *Stream) castRecordFieldsIfNeeded(record map[string]any) map[string]any {
	if s.entity == "" {
		return record
	}
	if _, found := record["properties"]; !found {
		return record
	}

	properties, err := s.properties()
	if err != nil {
		return record
	}

	if recordProperties, ok := record["properties"].(map[string]any); ok {
		for fieldName, fieldValue := range recordProperties {
			if _, found := properties[fieldName]; !found {
				logger.Warnf("Property discarded: not maching with properties schema: record id:%v, property_value: %s", record["id"], fieldName)
				continue
			}
			declaredFieldTypes := properties[fieldName].Type
			format := properties[fieldName].Format
			reformattedFieldValue, err := utils.ReformatValueOnDataTypes(declaredFieldTypes, format, fieldValue)
			if err != nil {
				logger.Warnf("failed to reformat for field[%s] to data-type:%v for record id:%v", fieldName, declaredFieldTypes, record["id"])
				continue
			}
			recordProperties[fieldName] = reformattedFieldValue
		}
	}

	return record
}

func (s *Stream) trasformSingleRecord(record map[string]any) map[string]any {
	// Preprocess a single record
	record = s.castRecordFieldsIfNeeded(record)
	if s.createdAtField != "" && s.updatedAtField != "" && record[s.updatedAtField] == nil {
		record[s.updatedAtField] = record[s.createdAtField]
	}

	return record
}

func (s *Stream) transform(records <-chan map[string]any) <-chan map[string]any {
	// Preprocess record before emitting
	stream := make(chan map[string]any)
	go func() {
		for record := range records {
			record = s.castRecordFieldsIfNeeded(record)
			if s.createdAtField != "" && s.updatedAtField != "" && record[s.updatedAtField] == nil {
				record[s.updatedAtField] = record[s.createdAtField]
			}

			stream <- record
		}
	}()

	return stream
}

func (s *Stream) filterOldRecords(records <-chan map[string]any) <-chan map[string]any {
	stream := make(chan map[string]any)
	go func() {
		for record := range records {
			if uat, found := record[s.updatedAtField]; found {
				updatedAt, err := utils.ReformatDate(uat)
				if err != nil {
					logger.Warnf("failed to reformat[%s] for record %v: %s", s.updatedAtField, record, err)
					continue
				}

				if updatedAt.Before(s.startDate) {
					continue
				}
			}

			stream <- record
		}

		close(stream)
	}()
	return stream
}

func (s *Stream) flatAssociations(records <-chan map[string]any) <-chan map[string]any {
	// When result has associations we prefer to have it flat, so we transform this

	// "associations": {
	// 	"contacts": {
	// 		"results": [{"id": "201", "type": "company_to_contact"}, {"id": "251", "type": "company_to_contact"}]}
	// 	}
	// }

	// to this:

	// "contacts": [201, 251]

	stream := make(chan map[string]any, s.batchSize)
	go func() {
		for record := range records {
			if value, found := record["associations"]; found {
				delete(record, "associations")
				if associations, ok := value.(map[string]any); ok {
					for name, value := range associations {
						tempArray := []any{}
						if association, ok := value.(map[string]any); ok {
							if value, found := association["results"]; found {
								if results, ok := value.([]any); ok {
									for _, value := range results {
										if row, ok := value.(map[string]any); ok {
											tempArray = append(tempArray, row["id"])
										}
									}
								}

							}
						}
						record[strings.ReplaceAll(name, " ", "_")] = tempArray
					}
				}
			}

			// insert record
			stream <- record
		}

		close(stream)
	}()

	return stream
}

func (s *Stream) handleRequest(nextPageToken map[string]any, properties map[string]string, urn string) (int, []byte, error) {
	if urn == "" {
		urn = s.path()
	}
	req, err := http.NewRequest("GET", formatEndpoint(s.path()), nil)
	resp, err := s.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, nil, utils.ErrServerTimeout
		}
		urlErr, ok := err.(*url.Error)
		if ok && urlErr.Timeout() {
			return 0, nil, utils.ErrServerTimeout
		}

		return 0, nil, fmt.Errorf("Error getting response: %v", err)
	}
	defer func() {
		if resp.Body != nil {
			resp.Body.Close()
		}
	}()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, fmt.Errorf("Error reading response: %v", err)
	}

	return resp.StatusCode, respBody, nil
}