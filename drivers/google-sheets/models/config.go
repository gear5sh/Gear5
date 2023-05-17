package models

type Credentials struct {
	ClientID     string `json:"client_id" required:"true"`
	ClientSecret string `json:"client_secret" required:"true"`
	RefreshToken string `json:"refresh_token"`
}

type Config struct {
	Credentials    *Credentials `json:"credentials" required:"true"`
	SpreadsheetID  string       `json:"spreadsheet_id" required:"true"`
	NameConversion *bool        `json:"name_conversion" required:"false"`
}
