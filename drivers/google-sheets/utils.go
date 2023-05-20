package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	unidecode "github.com/mozillazg/go-unidecode"
	"github.com/piyushsingariya/syndicate/constants"
	"github.com/piyushsingariya/syndicate/drivers/google-sheets/models"
	syndicatemodels "github.com/piyushsingariya/syndicate/models"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2/google"
	"gopkg.in/Iwark/spreadsheet.v2"
)

const DefaultSeparator = "_"

func NewClient(config *models.Config) (*spreadsheet.Service, error) {
	// create api context
	ctx := context.Background()

	// get bytes from base64 encoded google service accounts key
	credBytes, err := json.Marshal(map[string]string{
		// "client_id":     config.Credentials.OAuth.ClientID,
		// "refresh_token": config.Credentials.OAuth.RefreshToken,
		// "client_secret": config.Credentials.OAuth.ClientSecret,
	})
	if err != nil {
		return nil, err
	}

	// authenticate and get configuration
	jwtConfig, err := google.JWTConfigFromJSON(credBytes, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		return nil, err
	}

	// create client with config and context
	confClient := jwtConfig.Client(ctx)

	service := spreadsheet.NewServiceWithClient(confClient)

	// fetch spreadsheet
	_, err = service.FetchSpreadsheet(config.SpreadsheetID)
	if err != nil {
		return nil, err
	}

	return service, nil
}

func LoadHeaders(sheet spreadsheet.Sheet) ([]string, error) {
	if len(sheet.Rows) == 0 {
		return []string{}, fmt.Errorf("expected data for exactly one range for sheet %s", sheet.Properties.Title)
	}

	if sheet.Properties.GridProperties.ColumnCount == 0 {
		logrus.Warn("[%s] The sheet %s (ID %s) columns are empty!", EmptySheetError, sheet.Properties.Title, sheet.Properties.ID)
		return []string{}, nil
	}

	if sheet.Properties.GridProperties.RowCount == 0 {
		logrus.Warn("[%s] The sheet %s (ID %s) rows are empty!", EmptySheetError, sheet.Properties.Title, sheet.Properties.ID)
		return []string{}, nil
	}

	if sheet.Properties.GridProperties.RowCount == 1 {
		logrus.Warn("[%s] no data found in the sheet %s (ID %s) rows are empty!", EmptySheetError, sheet.Properties.Title, sheet.Properties.ID)
		return []string{}, nil
	}

	var headers []string

	for _, pointer := range sheet.Rows[0] {
		headers = append(headers, pointer.Value)
	}

	return headers, nil
}

func GetValidHeadersAndDuplicates(headers []string) ([]string, []string) {
	fields := []string{}
	duplicateHeaders := []string{}
	set := make(map[string]bool)

	for _, header := range headers {
		if _, found := set[header]; found {
			duplicateHeaders = append(duplicateHeaders, header)
			fields = append(fields, header)
		} else {
			set[header] = true
			fields = append(fields, header)
		}
	}

	return fields, duplicateHeaders
}

func SafeNameConversion(text string) (string, error) {
	//   convert name using a set of rules, for example: '1MyName' -> '_1_my_name'
	pattern, err := regexp.Compile("[A-Z]+[a-z]*|[a-z]+|\\d+|(?P<NoToken>[^a-zA-Z\\d]+)")
	if err != nil {
		return text, err
	}

	text = unidecode.Unidecode(text)

	tokens := []string{}
	for _, m := range pattern.FindAllStringSubmatch(text, -1) {
		if m[1] == "" {
			tokens = append(tokens, m[0])
		} else {
			tokens = append(tokens, "")
		}
	}

	if len(tokens) >= 3 {
		newTokens := []string{tokens[0]}
		for _, t := range tokens[1 : len(tokens)-1] {
			if t != "" {
				newTokens = append(newTokens, t)
			}
		}
		newTokens = append(newTokens, tokens[len(tokens)-1])
		tokens = newTokens
	}

	if len(tokens) > 0 {
		if _, err := strconv.Atoi(tokens[0]); err == nil {
			tokens = append([]string{""}, tokens...)
		}
	}

	text = strings.Join(tokens, DefaultSeparator)

	return strings.ToLower(text), nil
}

func headersToStream(sheetName string, headers []string) *syndicatemodels.Stream {
	stream := syndicatemodels.Stream{}
	stream.Name = sheetName
	stream.JsonSchema = &syndicatemodels.Schema{}

	for _, header := range headers {
		stream.JsonSchema.Properties[header] = &syndicatemodels.Property{
			// for simplicity, every field is a string
			Type: []constants.DataType{constants.String},
		}
	}

	return &stream
}

func GetIndexToColumn(sheet spreadsheet.Sheet) (map[int]string, error) {
	headers, err := LoadHeaders(sheet)
	if err != nil {
		return nil, err
	}

	output := make(map[int]string)

	for i := range headers {
		headers[i], err = SafeNameConversion(headers[i])
		if err != nil {
			return nil, err
		}

		output[i] = headers[i]
	}

	return output, nil
}
