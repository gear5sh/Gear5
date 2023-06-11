package driver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
				Name:      name,
				Namespace: namespace,
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

	return properties, nil
}

func (s *Stream) propertiesScopeIsGranted() bool {
	return utils.IsSubset(s.grantedScopes, s.propertiesScopes)
}

func (s *Stream) Read(channel <-chan syndicatemodels.Record) error {
	return fmt.Errorf("no implementation on base stream")
}

func (s *Stream) getStream() *syndicatemodels.Stream {
	return s.Stream
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

// def _cast_record_fields_if_needed(self, record: Mapping, properties: Mapping[str, Any] = None) -> Mapping:
//         if not self.entity or not record.get("properties"):
//             return record

//         properties = properties or self.properties

//         for field_name, field_value in record["properties"].items():
//             if field_name not in properties:
//                 self.logger.info(
//                     "Property discarded: not maching with properties schema: record id:{}, property_value: {}".format(
//                         record.get("id"), field_name
//                     )
//                 )
//                 continue
//             declared_field_types = properties[field_name].get("type", [])
//             if not isinstance(declared_field_types, Iterable):
//                 declared_field_types = [declared_field_types]
//             format = properties[field_name].get("format")
//             record["properties"][field_name] = self._cast_value(
//                 declared_field_types=declared_field_types, field_name=field_name, field_value=field_value, declared_format=format
//             )

//         return record

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

// def _transform_single_record(self, record: Mapping) -> Mapping:
//         """Preprocess a single record"""
//         record = self._cast_record_fields_if_needed(record)
//         if self.created_at_field and self.updated_at_field and record.get(self.updated_at_field) is None:
//             record[self.updated_at_field] = record[self.created_at_field]
//         return record

//     def _transform(self, records: Iterable) -> Iterable:
//         """Preprocess record before emitting"""
//         for record in records:
//             record = self._cast_record_fields_if_needed(record)
//             if self.created_at_field and self.updated_at_field and record.get(self.updated_at_field) is None:
//                 record[self.updated_at_field] = record[self.created_at_field]
//             yield record

func (s *Stream) trasformSingleRecord(record map[string]any) map[string]any {
	// Preprocess a single record

	//         record = self._cast_record_fields_if_needed(record)
	//         if self.created_at_field and self.updated_at_field and record.get(self.updated_at_field) is None:
	//             record[self.updated_at_field] = record[self.created_at_field]
	//         return record
}

func (s *Stream) filterOldRecords(records <-chan map[string]any) chan<- map[string]any {
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

func (s *Stream) flatAssociations(records <-chan map[string]any) chan<- map[string]any {
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
