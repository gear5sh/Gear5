package models

import (
	"fmt"

	"github.com/piyushsingariya/syndicate"
)

//go:generate jsonschemagen -s -c -r -o ./petschema github.com/piyushsingariya/syndicate/driver/google-sheets/models Config
type Config struct {
	// Credentials for connecting to the Google Sheets API
	//
	// @jsonschema(
	// required=true,
	// oneOf=["github.com/piyushsingariya/syndicate/drivers/google-sheets/models/Client","github.com/piyushsingariya/syndicate/drivers/google-sheets/models/Service"]
	// )
	Credentials    interface{} `json:"credentials"  jsonschema:"oneoftype=Client;Service,title=Authentication,description=Credentials for connecting to the Google Sheets API"`
	SpreadsheetID  string      `json:"spreadsheet_id" required:"true"`
	NameConversion *bool       `json:"name_conversion" required:"false" default:"true"`
	_              struct{}    `additionalProperties:"true"`       // Tags of unnamed field are applied to parent schema.
	_              struct{}    `title:"Google Sheet driver spec" ` // Multiple unnamed fields can be used.
}

type Credentials struct {
	Client  *Client
	Service *Service `title:"Service Account Key Authentication" jsonschema:"oneof_required=Service"`
}

type Client struct {
	// @jsonschema=(title=Authenticate via Google (OAuth))
	_ struct{} `jsonschema:"title=Authenticate via Google (OAuth)"`
	// Auth Type.
	//
	// @jsonSchema(required=true, maxLength=250)
	AuthType     string `json:"auth_type" jsonschema:"title=Auth Type,const=Client"`
	ClientID     string `json:"client_id" jsonschema:"title=Client ID,description=Enter your Google application's Client ID"`
	ClientSecret string `json:"client_secret" jsonschema:"title=Client Secret,description=Enter your Google application's Client Secret"`
	RefreshToken string `json:"refresh_token" title:"Refresh Token" required:"true" description:"Enter your Google application's refresh token"`
}

type Service struct {
	// @jsonschema=(title=Service Account Key Authentication)
	_ struct{} `jsonschema:"title=Service Account Key Authentication"`
	// Auth Type.
	//
	// @jsonSchema(required=true, maxLength=250)
	AuthType           string `json:"auth_type" jsonschema:"title=Auth Type,const=Service"`
	ServiceAccountInfo string `json:"service_account_info" title:"Service Account Information." required:"true" description:"Enter your Google Cloud <a href='https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating_service_account_keys'>service account key</a> in JSON format" example:"'{ 'type': 'service_account', 'project_id': YOUR_PROJECT_ID, 'private_key_id': YOUR_PRIVATE_KEY, ... }'"`
}

func (c *Config) ValidateAndPopulateDefaults() error {
	if c.NameConversion == nil {
		c.NameConversion = syndicate.Bool(true)
	}

	if c.Credentials == nil {
		return fmt.Errorf("credentials not found")
	}

	return nil
}
