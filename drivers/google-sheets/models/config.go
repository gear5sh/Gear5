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
	// Credentials    *Credentials `json:"credentials"  jsonschema:"oneof_type=Client;Service,title=Authentication,description=Credentials for connecting to the Google Sheets API"`
	Credentials    interface{} `json:"credentials"`
	SpreadsheetID  string      `json:"spreadsheet_id" required:"true"`
	NameConversion *bool       `json:"name_conversion" required:"false" default:"true"`
}

type Credentials struct {
	Service *Service `title:"Service Account Key Authentication,oneof_required=Service"`
}

// @jsonschema=(title=Authenticate via Google (OAuth))
type Client struct {
	// Auth Type.
	//
	// @jsonSchema(required=true, maxLength=250, const=Client)
	AuthType     string `json:"auth_type" jsonschema:"title=Auth Type,const=Client"`
	ClientID     string `json:"client_id" jsonschema:"title=Client ID,description=Enter your Google application's Client ID"`
	ClientSecret string `json:"client_secret" jsonschema:"title=Client Secret,description=Enter your Google application's Client Secret"`
	RefreshToken string `json:"refresh_token" title:"Refresh Token" required:"true" description:"Enter your Google application's refresh token"`
}

// @jsonschema=(title=Service Account Key Authentication)
type Service struct {
	// Auth Type.
	//
	// @jsonSchema(required=true, const=Client)
	AuthType           string `json:"auth_type"`
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
