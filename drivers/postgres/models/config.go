package models

import (
	"fmt"
	"strings"
)

type Config struct {
	// Hostname of the database.
	//
	// @jsonschema(
	// required=true
	// )
	Host string `json:"host"`
	// Port of the database.
	//
	// @jsonschema(
	// required=true,
	//  minimum=0,
	//  maximum=65536,
	//  default=5432
	// )
	Port int `json:"port"`
	// Hostname of the database.
	//
	// @jsonschema(
	// required=true
	// )
	Database string `json:"database"`

	// user of the database.
	//
	// @jsonschema(
	// required=true
	// )
	Username string `json:"username"`
	// password of the user.
	//
	// @jsonschema(
	// required=true
	// )
	Password string `json:"password"`
	// Additional properties to pass to the JDBC URL string when connecting to the database formatted as 'key=value' pairs separated by the symbol '&'. (Eg. key1=value1&key2=value2&key3=value3). For more information read about <a href=\"https://jdbc.postgresql.org/documentation/head/connect.html\">JDBC URL parameters</a>.
	//
	// @jsonschema(
	// required=true,
	// title=JDBC URL Parameters (Advanced),
	// pattern=key1=value1&key2=value2
	// )
	JDBCURLParams string `json:"jdbc_url_params"`
	// Hostname of the database.
	//
	// @jsonschema(
	// required=true
	// )
	// SSLMode string `json:"database"`
}

func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("empty host name")
	} else if strings.Contains(c.Host, "https") || strings.Contains(c.Host, "http") {
		return fmt.Errorf("host should not contain http or https")
	}

	return nil
}

func (c *Config) ToConnectionString() string {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s %s",
		c.Host, c.Port, c.Database, c.Username, c.Password, c.JDBCURLParams)

	return connStr
}
