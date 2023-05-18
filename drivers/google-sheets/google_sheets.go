package main

import (
	"fmt"
	"strings"

	"github.com/piyushsingariya/syndicate/drivers/google-sheets/models"
	"github.com/piyushsingariya/syndicate/logger"
	syndicatemodels "github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/utils"
	"github.com/piyushsingariya/syndicate/utils/jsonutils"
	"gopkg.in/Iwark/spreadsheet.v2"
)

type GoogleSheets struct {
	*spreadsheet.Service
	config    *models.Config
	catalog   *syndicatemodels.ConfiguredCatalog
	batchSize int
}

func (gs *GoogleSheets) Setup(config, _, catalog interface{}, batchSize int) error {
	conf := &models.Config{}
	if err := jsonutils.UnmarshalConfig(config, conf); err != nil {
		return err
	}

	if err := conf.ValidateAndPopulateDefaults(); err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	if catalog != nil {
		cat := &syndicatemodels.ConfiguredCatalog{}
		if err := jsonutils.UnmarshalConfig(catalog, cat); err != nil {
			return err
		}

		gs.catalog = cat
	}

	client, err := NewClient(conf)
	if err != nil {
		return err
	}
	gs.config = conf
	gs.Service = client
	gs.batchSize = batchSize

	return nil
}

func (gs *GoogleSheets) Check() error {
	_, _, err := gs.getAllSheetStreams()
	if err != nil {
		return err
	}

	return nil
}

func (gs *GoogleSheets) Discover() ([]*syndicatemodels.Stream, error) {
	streams, _, err := gs.getAllSheetStreams()
	if err != nil {
		return nil, err
	}

	return streams, nil
}

func (gs *GoogleSheets) Read(channel chan<- syndicatemodels.RecordRow) error {
	spreadsheetID := gs.config.SpreadsheetID
	batchSize := gs.batchSize
	totalRecords := 0
	records := []syndicatemodels.RecordRow{}

	logger.Info("Starting sync for spreadsheet [%s]", spreadsheetID)

	_, streamNamesToSheet, err := gs.getAllSheetStreams()
	if err != nil {
		return err
	}

	selectedStreams := utils.GetStreamNamesFromConfiguredCatalog(gs.catalog)
	for _, stream := range selectedStreams {
		sheet, found := streamNamesToSheet[stream]
		if !found {
			logger.Info("sheet not found with stream name [%s] in spreadsheet; skipping", stream)
			continue
		}

		indexToHeaders, err := GetIndexToColumn(sheet)
		if err != nil {
			return fmt.Errorf("failed to mark headers to index: %s", err)
		}

		logger.Infof("Row count in sheet %s[%s]:%d", sheet.Properties.Title, sheet.Properties.ID, sheet.Properties.GridProperties.RowCount-1)

		for rowCursor := 1; rowCursor < len(sheet.Rows); rowCursor += batchSize {
			// make a batch of records
			for batchCursor := rowCursor; batchCursor < len(sheet.Rows) && batchCursor < rowCursor+batchSize; batchCursor++ {
				record := syndicatemodels.RecordRow{Stream: stream, Data: make(map[string]interface{})}

				for i, pointer := range sheet.Rows[batchCursor] {
					record.Data[indexToHeaders[i]] = pointer.Value
				}

				records = append(records, record)
			}

			// flush the records after collecting the batch
			if len(records) >= batchSize {
				for _, record := range records {
					channel <- record
				}

				// reset
				records = []syndicatemodels.RecordRow{}
				totalRecords += len(records)
			}
		}
	}

	// flush pending records
	if len(records) > 0 {
		for _, record := range records {
			channel <- record
		}

		// reset
		records = []syndicatemodels.RecordRow{}
		totalRecords += len(records)
	}

	logger.Infof("Total records fetched %d", totalRecords)

	return err
}

func (gs *GoogleSheets) getAllSheetStreams() ([]*syndicatemodels.Stream, map[string]spreadsheet.Sheet, error) {
	logger.Infof("fetching spreadsheet[%s]", gs.config.SpreadsheetID)
	googleSpreadsheet, err := gs.FetchSpreadsheet(gs.config.SpreadsheetID)
	if err != nil {
		return nil, nil, err
	}

	streams := []*syndicatemodels.Stream{}
	streamNameToSheet := make(map[string]spreadsheet.Sheet)
	for _, sheet := range googleSpreadsheet.Sheets {
		headers, err := LoadHeaders(sheet)
		if err != nil {
			if strings.Contains(err.Error(), EmptySheetError) {
				logger.Info("Skipping empty sheet: %s", err.Error())
				continue
			}
			return nil, nil, err
		}

		if gs.config.NameConversion != nil && *gs.config.NameConversion {
			for i := range headers {
				headers[i], err = SafeNameConversion(headers[i])
				if err != nil {
					logger.Errorf("failed to safely convert header %s: %s", headers[i], err)
				}
			}
		}

		headers, duplicateHeaders := GetValidHeadersAndDuplicates(headers)
		if len(duplicateHeaders) > 0 {
			return nil, nil, fmt.Errorf("found duplicate headers in Sheet[%s]: %s", sheet.Properties.Title, strings.Join(duplicateHeaders, ", "))
		}

		streams = append(streams, headersToStream(sheet.Properties.Title, headers))
	}

	return streams, streamNameToSheet, nil
}
