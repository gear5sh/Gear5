package models

import (
	"fmt"

	"github.com/piyushsingariya/syndicate"
)

type Config struct {
	Credentials    *Credentials `json:"credentials" required:"true" title:"Authentication" description:"Credentials for connecting to the Google Sheets API"`
	SpreadsheetID  string       `json:"spreadsheet_id" required:"true"`
	NameConversion *bool        `json:"name_conversion" required:"false" default:"true"`
	_              struct{}     `additionalProperties:"true"`       // Tags of unnamed field are applied to parent schema.
	_              struct{}     `title:"Google Sheet driver spec" ` // Multiple unnamed fields can be used.
}

type Credentials struct {
	ClientID     string `json:"client_id" title:"Client ID" required:"true" description:"Enter your Google application's Client ID"`
	ClientSecret string `json:"client_secret" title:"Client Secret" required:"true" description:"Enter your Google application's Client Secret"`
	RefreshToken string `json:"refresh_token" title:"Refresh Token" required:"true" description:"Enter your Google application's refresh token"`
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
