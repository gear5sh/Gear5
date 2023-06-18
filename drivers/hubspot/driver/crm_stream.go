package driver

import (
	"fmt"
	"net/http"
	"time"

	"github.com/piyushsingariya/syndicate/logger"
	syndicatemodels "github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/safego"
	"github.com/piyushsingariya/syndicate/types"
	"github.com/piyushsingariya/syndicate/typing"
	"github.com/piyushsingariya/syndicate/utils"
)

type CRMSearchStream struct {
	IncrementalStream
	associations []string
}

func newCRMSearchStream(incrementalStream IncrementalStream, associations []string) *CRMSearchStream {
	return &CRMSearchStream{
		IncrementalStream: incrementalStream,
		associations:      associations,
	}
}

func (c *CRMSearchStream) path() (string, string) {
	if c._state != nil {
		return fmt.Sprintf("/crm/v3/objects/%s/search", c.entity), http.MethodPost
	}
	return fmt.Sprintf("/crm/v3/objects/%s", c.entity), http.MethodGet
}

func (c *CRMSearchStream) search() (any, []byte, error) {
	// TODO:

	return nil, nil, nil
}

func (c *CRMSearchStream) processSearch(nextPageToken map[string]any) ([]map[string]any, []byte, error) {
	streamRecords := []map[string]any{}
	properties, err := c.propertiesList()
	if err != nil {
		return nil, nil, err
	}
	payload := map[string]any{}
	if c._state != nil {
		payload = map[string]any{
			"filters": []map[string]any{
				{"value": int(c._state.Unix() * 1000), "propertyName": c.lastModifiedField, "operator": "GTE"},
			},
			"sorts": []map[string]any{
				{"propertyName": c.lastModifiedField, "direction": "ASCENDING"},
			},
			"properties": properties,
			"limit":      100,
		}
	}

	for key, value := range nextPageToken {
		payload[key] = value
	}

	_, rawResponse, err := c.search()
	if err != nil {
		return nil, nil, err
	}

	records, err := c.transform(c.parseResponse(rawResponse))
	if err != nil {
		return nil, nil, err
	}

	for record := range records {
		streamRecords = append(streamRecords, record)
	}

	return streamRecords, rawResponse, nil
}

func (c *CRMSearchStream) readRecords(send chan<- syndicatemodels.Record) error {
	paginationComplete := false
	var nextPageToken map[string]any
	latest_cursor := &time.Time{}

	for !paginationComplete {
		var records []types.RecordData
		var rawResponse any
		var err error

		if c._state != nil {
			records, rawResponse, err = c.processSearch(nextPageToken)
			if err != nil {
				return err
			}
		} else {
			records, rawResponse, err = c.readStreamRecords(nextPageToken, c.path)
			if err != nil {
				return err
			}
		}

		for record := range c.filterOldRecords(c.flatAssociations(utils.ToChannel(records, 0))) {
			cursor, err := typing.ReformatDate(record[c.updatedAtField])
			if err != nil {
				return err
			}
			if latest_cursor != nil {
				latest_cursor = utils.Time(utils.MaxDate(cursor, *latest_cursor))
			} else {
				latest_cursor = &cursor
			}

			if !safego.Insert(send, c.reformatRecord(record)) {
				// channel was closed
				return nil
			}
		}

		nextPageToken, err = c.nextPageToken(rawResponse)
		if err != nil {
			logger.Warnf("Error occured while getting next page token for stream %s: %s", c.Name(), err)
			paginationComplete = true
		} else if c._state != nil && nextPageToken["after"].(int) >= 10000 {
			// Hubspot documentation states that the search endpoints are limited to 10,000 total results
			// for any given query. Attempting to page beyond 10,000 will result in a 400 error.
			// https://developers.hubspot.com/docs/api/crm/search. We stop getting data at 10,000 and
			// start a new search query with the latest state that has been collected.
			c.updateState(*latest_cursor, false)
			nextPageToken = nil
		}
	}

	c.updateState(*latest_cursor, true)
	return nil
}
