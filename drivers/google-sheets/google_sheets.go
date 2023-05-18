package main

import (
	"fmt"
	"strings"

	"github.com/piyushsingariya/syndicate/drivers/google-sheets/models"
	syndicatemodels "github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/utils"
	"github.com/piyushsingariya/syndicate/utils/jsonutils"
	"github.com/sirupsen/logrus"
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
	spreadsheet, err := gs.FetchSpreadsheet(gs.config.SpreadsheetID)
	if err != nil {
		return err
	}

	for _, sheet := range spreadsheet.Sheets {
		headers, err := LoadHeaders(sheet)
		if err != nil {
			if strings.Contains(err.Error(), EmptySheetError) {
				logrus.Info("Skipping empty sheet: %s", err.Error())
				continue
			}
			return err
		}

		if gs.config.NameConversion != nil && *gs.config.NameConversion {
			for i := range headers {
				headers[i], err = SafeNameConversion(headers[i])
				if err != nil {
					logrus.Errorf("failed to safely convert header %s: %s", headers[i], err)
				}
			}
		}

		headers, duplicateHeaders := GetValidHeadersAndDuplicates(headers)

		if len(duplicateHeaders) > 0 {
			return fmt.Errorf("found duplicate headers in Sheet[%s]: %s", sheet.Properties.Title, strings.Join(duplicateHeaders, ", "))
		}
	}

	return nil
}

func (gs *GoogleSheets) Discover() ([]*syndicatemodels.Stream, error) {
	spreadsheet, err := gs.FetchSpreadsheet(gs.config.SpreadsheetID)
	if err != nil {
		return nil, err
	}

	streams := []*syndicatemodels.Stream{}

	for _, sheet := range spreadsheet.Sheets {
		headers, err := LoadHeaders(sheet)
		if err != nil {
			if strings.Contains(err.Error(), EmptySheetError) {
				logrus.Info("Skipping empty sheet: %s", err.Error())
				continue
			}
			return nil, err
		}

		if gs.config.NameConversion != nil && *gs.config.NameConversion {
			for i := range headers {
				headers[i], err = SafeNameConversion(headers[i])
				if err != nil {
					logrus.Errorf("failed to safely convert header %s: %s", headers[i], err)
				}
			}
		}

		headers, duplicateHeaders := GetValidHeadersAndDuplicates(headers)
		if len(duplicateHeaders) > 0 {
			return nil, fmt.Errorf("found duplicate headers in Sheet[%s]: %s", sheet.Properties.Title, strings.Join(duplicateHeaders, ", "))
		}

		streams = append(streams, headersToStream(sheet.Properties.Title, headers))
	}

	return streams, nil
}

func (gs *GoogleSheets) Read(channel chan<- syndicatemodels.RecordRow) error {
	sheetsFromCatalog := utils.StreamNamesConfiguredCatalog(gs.catalog)
	spreadsheetID := gs.config.SpreadsheetID
	batchSize := gs.batchSize

	logrus.Info("Starting sync for spreadsheet [%s]", spreadsheetID)

	spreadsheet, err := gs.FetchSpreadsheet(spreadsheetID)
	if err != nil {
		return err
	}

	for _, sheet := range spreadsheet.Sheets {
		headers, err := LoadHeaders(sheet)
		if err != nil {
			if strings.Contains(err.Error(), EmptySheetError) {
				logrus.Info("Skipping empty sheet: %s", err.Error())
				continue
			}
			return err
		}

		if gs.config.NameConversion != nil && *gs.config.NameConversion {
			for i := range headers {
				headers[i], err = SafeNameConversion(headers[i])
				if err != nil {
					logrus.Errorf("failed to safely convert header %s: %s", headers[i], err)
				}
			}
		}

		headers, duplicateHeaders := GetValidHeadersAndDuplicates(headers)

		if len(duplicateHeaders) > 0 {
			return fmt.Errorf("found duplicate headers in Sheet[%s]: %s", sheet.Properties.Title, strings.Join(duplicateHeaders, ", "))
		}
	}
}
