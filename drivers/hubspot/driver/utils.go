package driver

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/piyushsingariya/syndicate/drivers/hubspot/models"
	"github.com/piyushsingariya/syndicate/logger"
	syndicatemodels "github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/types"
	"github.com/piyushsingariya/syndicate/utils"
	"golang.org/x/oauth2"
)

var ValidJsonSchemaTypes = []types.DataType{
	types.String,
	types.Integer,
	types.Number,
	types.Boolean,
	types.Object,
	types.Array,
}

var KnownConvertibleSchemaTypes = map[string]syndicatemodels.Property{
	"bool":         {Type: []types.DataType{types.Boolean}},
	"enumeration":  {Type: []types.DataType{types.String}},
	"date":         {Type: []types.DataType{types.String}, Format: "date"},
	"date-time":    {Type: []types.DataType{types.String}, Format: "date-time"},
	"datetime":     {Type: []types.DataType{types.String}, Format: "date-time"},
	"json":         {Type: []types.DataType{types.String}},
	"phone_number": {Type: []types.DataType{types.String}},
}

var CustomFieldTypeToValue = map[reflect.Type]string{
	reflect.TypeOf(true):     "boolean",
	reflect.TypeOf(""):       "string",
	reflect.TypeOf(2.000000): "number",
	reflect.TypeOf(1):        "integer",
}

var CustomFieldValueToType []string

const BaseURL = "https://api.hubapi.com"

func formatEndpoint(urn string) string {
	return fmt.Sprintf("%s/%s", strings.TrimSuffix(BaseURL, "/"), strings.TrimPrefix(urn, "/"))
}

func newClient(config *models.Config) (*http.Client, string, error) {
	var client *http.Client
	var accessToken string
	if ok, _ := utils.IsOfType(config.Credentials, "client_id"); ok {
		logger.Info("Credentials found to be OAuth")
		oauth := &models.Client{}
		if err := utils.Unmarshal(config.Credentials, oauth); err != nil {
			return nil, "", err
		}

		// Create a new OAuth2 config
		config := &oauth2.Config{
			ClientID:     oauth.ClientID,
			ClientSecret: oauth.ClientSecret,
			Endpoint: oauth2.Endpoint{
				TokenURL: formatEndpoint("/oauth/v1/token"),
			},
		}

		// Create a new token source using the refresh token
		tokenSource := config.TokenSource(context.TODO(), &oauth2.Token{
			RefreshToken: oauth.RefreshToken,
		})

		token, err := tokenSource.Token()
		if err != nil {
			return nil, "", fmt.Errorf("failed to retrive access token from refresh token: %s", err)
		}

		accessToken = token.AccessToken

		// Create a new OAuth2 client
		client = oauth2.NewClient(context.TODO(), tokenSource)
	} else if ok, _ := utils.IsOfType(config.Credentials, "access_token"); ok {
		logger.Info("Credentials found to be Private App")
		privateApp := &models.PrivateApp{}
		err := utils.Unmarshal(config.Credentials, privateApp)
		if err != nil {
			return nil, "", err
		}

		// Create a new OAuth2 config
		config := &oauth2.Config{
			Endpoint: oauth2.Endpoint{
				TokenURL: formatEndpoint("/oauth/v1/token"),
			},
		}

		// Create a new token source using the refresh token
		tokenSource := config.TokenSource(context.TODO(), &oauth2.Token{
			AccessToken: privateApp.AccessToken,
		})

		accessToken = privateApp.AccessToken

		// Create a new OAuth2 client
		client = oauth2.NewClient(context.TODO(), tokenSource)
	} else {
		return nil, "", fmt.Errorf("invalid credentials format, expected formats are: %T and %T", models.Client{}, models.PrivateApp{})
	}

	if client == nil {
		return nil, "", fmt.Errorf("failed to create hubspot authorized client")
	}

	return client, accessToken, nil
}

func getFieldProps(fieldType string) *syndicatemodels.Property {
	if utils.ArrayContains(ValidJsonSchemaTypes, fieldType) {
		return &syndicatemodels.Property{
			Type: []types.DataType{types.DataType(fieldType)},
		}
	}

	if property, found := KnownConvertibleSchemaTypes[fieldType]; !found {
		return &property
	} else {
		return &syndicatemodels.Property{
			Type: []types.DataType{types.DataType(fieldType)},
		}
	}
}
