	// GithubComPiyushsingariyaSyndicateDriversGoogleSheetsModelsConfig is a json-schema accessor
	GithubComPiyushsingariyaSyndicateDriversGoogleSheetsModelsConfig = `{"$schema":"http://json-schema.org/draft-04/schema#","type":"object","definitions":{"github_com-piyushsingariya-syndicate-drivers-google-sheets-models-Client":{"type":"object","properties":{"auth_type":{"type":"string","title":"Auth Type.","maxLength":250},"client_id":{"type":"string"},"client_secret":{"type":"string"},"refresh_token":{"type":"string"}},"required":["auth_type"],"x-go-path":"github.com/piyushsingariya/syndicate/drivers/google-sheets/models/Client"},"github_com-piyushsingariya-syndicate-drivers-google-sheets-models-Service":{"type":"object","properties":{"auth_type":{"type":"string","title":"Auth Type.","maxLength":250},"service_account_info":{"type":"string"}},"required":["auth_type"],"x-go-path":"github.com/piyushsingariya/syndicate/drivers/google-sheets/models/Service"}},"properties":{"credentials":{"type":"object","title":"Credentials for connecting to the Google Sheets API","oneOf":[{"$ref":"#/definitions/github_com-piyushsingariya-syndicate-drivers-google-sheets-models-Client"},{"$ref":"#/definitions/github_com-piyushsingariya-syndicate-drivers-google-sheets-models-Service"}]},"name_conversion":{"type":"boolean"},"spreadsheet_id":{"type":"string"}},"required":["credentials"],"x-go-path":"github.com/piyushsingariya/syndicate/drivers/google-sheets/models/Config"}`

	// GithubComPiyushsingariyaSyndicateDriversGoogleSheetsModelsClient is a json-schema accessor
	GithubComPiyushsingariyaSyndicateDriversGoogleSheetsModelsClient = `null`

	// GithubComPiyushsingariyaSyndicateDriversGoogleSheetsModelsService is a json-schema accessor
	GithubComPiyushsingariyaSyndicateDriversGoogleSheetsModelsService = `null`

)
