package models

import (
	"fmt"

	"github.com/go-playground/validator"
)

// Authenticate via Access and Secret Keys
type BaseAWS struct {
	// AccessKey for AWS
	//
	// @jsonschema(
	// required=true
	// )
	AccessKey string `json:"access_key" validate:"required"`
	// SecretAccessKey for AWS
	//
	// @jsonschema(
	// required=true
	// )
	SecretAccessKey string `json:"secret_access_key" validate:"required"`
	// Region for AWS
	//
	// @jsonschema(
	// required=true,
	// enum=["require","disable","verify-ca","verify-full"]
	// )
	Region string `json:"region" validate:"required"`
}

// Authenticate via AssumeRole in foreign Account
type AssumeRoleAWS struct {
	BaseAWS
	// Remote AccountID for AWS
	//
	// @jsonschema(
	// required=true
	// )
	AccountID string `json:"account_id"`
	// RoleName to assume in given AccountID for AWS
	//
	// @jsonschema(
	// required=true
	// )
	RoleName string `json:"role_name"`
}

type Config struct {
	// Target Stream Name
	//
	// @jsonschema(
	// required=true
	// )
	TargetStreamName string `json:"stream_name" validate:"required"`
	// FileType
	//
	// @jsonschema(
	// required=true
	// )
	Type string `json:"type" validate:"required"`
	// Bucket Name
	//
	// @jsonschema(
	// required=true
	// )
	Bucket string `json:"bucket" validate:"required"`
	// Pattern for bucket
	//
	// @jsonschema(
	// required=true
	// )
	Pattern string `json:"pattern" validate:"required"`
	// Bucket Region for AWS
	//
	// @jsonschema(
	// required=true
	// )
	Region string `json:"region" validate:"required"`
	// Credentials for connecting to AWS
	//
	// @jsonschema(
	// required=true,
	// oneOf=["BaseAWS","AssumeRoleAWS"]
	// )
	Credentials interface{} `json:"credentials" validate:"required"`
}

func (c *Config) Validate() error {
	validate := validator.New()
	err := validate.Struct(c)
	if err != nil {
		return fmt.Errorf("config validation failed: %s", err)
	}

	return nil
}
