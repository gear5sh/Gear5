package driver

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/piyushsingariya/drivers/s3/models"
	"github.com/piyushsingariya/kaku/jsonschema"
	"github.com/piyushsingariya/kaku/jsonschema/schema"
	kakumodels "github.com/piyushsingariya/kaku/models"
	protocol "github.com/piyushsingariya/kaku/protocol"
	"github.com/piyushsingariya/kaku/utils"
)

type S3 struct {
	config  *models.Config
	session *session.Session
	client  *s3.S3
}

func (s *S3) Setup(config any, catalog *kakumodels.Catalog, state kakumodels.State, batchSize int64) error {
	cfg := models.Config{}
	err := utils.Unmarshal(config, &cfg)
	if err != nil {
		return err
	}

	err = cfg.Validate()
	if err != nil {
		return err
	}

	s.session, err = newSession(cfg.Credentials)
	if err != nil {
		return err
	}
	s.client = s3.New(s.session)

	return nil
}

func (s *S3) Spec() (schema.JSONSchema, error) {
	return jsonschema.Reflect(models.Config{})
}

func (s *S3) Check() error {
	// Compile the regex pattern
	regexPattern, err := regexp.Compile(s.config.Pattern)
	if err != nil {
		return fmt.Errorf("Error compiling regex pattern: %s", err)
	}

	// List objects in the S3 bucket
	resp, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
	})
	if err != nil {
		return fmt.Errorf("Error listing objects: %s", err)
	}

	for _, obj := range resp.Contents {
		if regexPattern.MatchString(*obj.Key) || strings.HasSuffix(*obj.Key, fmt.Sprintf(".%s", s.config.Type)) {
			// get the file
			_, err := s.client.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(s.config.Bucket),
				Key:    obj.Key,
			})
			if err != nil {
				return fmt.Errorf("Error get file: %s", err)
			}
		}
	}

	return nil
}

func (s *S3) Discover() ([]*kakumodels.Stream, error)

func (s *S3) Catalog() *kakumodels.Catalog
func (s *S3) Type() string

func (s *S3) Streams() ([]*kakumodels.Stream, error)
func (s *S3) Read(stream protocol.Stream, channel chan<- kakumodels.Record) error
func (s *S3) GetState() (*kakumodels.State, error)
