package models

type Credentials struct {
	_ struct{} `additionalProperties:"false"`                                                             // Tags of unnamed field are applied to parent schema.
	_ struct{} `title:"Authentication" description:"Credentials for connecting to the Google Sheets API"` // Multiple unnamed fields can be used.

	ClientID     string `json:"client_id" title:"Client ID" required:"true" description:"Enter your Google application's Client ID"`
	ClientSecret string `json:"client_secret" title:"Client Secret" required:"true" description:"Enter your Google application's Client Secret"`
	RefreshToken string `json:"refresh_token" title:"Refresh Token" required:"true" description:"Enter your Google application's refresh token"`
}

type Config struct {
	Credentials    *Credentials `json:"credentials" required:"true"`
	SpreadsheetID  string       `json:"spreadsheet_id" required:"true"`
	NameConversion *bool        `json:"name_conversion" required:"false"`
}
