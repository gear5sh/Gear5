package driver

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/piyushsingariya/drivers/s3/models"
	"github.com/piyushsingariya/drivers/s3/reader"
	"github.com/piyushsingariya/kaku/jsonschema"
	"github.com/piyushsingariya/kaku/jsonschema/schema"
	kakumodels "github.com/piyushsingariya/kaku/models"
	protocol "github.com/piyushsingariya/kaku/protocol"
	"github.com/piyushsingariya/kaku/types"
	"github.com/piyushsingariya/kaku/utils"
	"tideland.dev/go/matcher"
)

type S3 struct {
	config    *models.Config
	session   *session.Session
	catalog   *kakumodels.Catalog
	state     kakumodels.State
	client    *s3.S3
	batchSize int64
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
	s.config = &cfg

	s.session, err = newSession(cfg.Credentials)
	if err != nil {
		return err
	}

	s.client = s3.New(s.session)
	s.batchSize = batchSize
	s.catalog = catalog
	s.state = state

	return nil
}

func (s *S3) Spec() (schema.JSONSchema, error) {
	return jsonschema.Reflect(models.Config{})
}

func (s *S3) Check() error {
	// List objects in the S3 bucket
	resp, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
	})
	if err != nil {
		return fmt.Errorf("Error listing objects: %s", err)
	}

	var schemas []map[string]*kakumodels.Property
	// iteration
	for resp.NextContinuationToken != nil {
		for _, obj := range resp.Contents {
			if matcher.Matches(s.config.Pattern, *obj.Key, matcher.ValidateCase) {
				reader, err := reader.Init(s.client, s.config.Type, s.config.Bucket, *obj.Key)
				if err != nil {
					return fmt.Errorf("failed to initialize reader on file[%s]: %s", *obj.Key, err)
				}
				schemas = append(schemas, reader.GetSchema())
			}
		}

		resp, err = s.client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(s.config.Bucket),
			ContinuationToken: resp.NextContinuationToken,
		})
		if err != nil {
			return fmt.Errorf("Error listing objects: %s", err)
		}
	}

	// final
	for _, obj := range resp.Contents {
		if matcher.Matches(s.config.Pattern, *obj.Key, matcher.ValidateCase) {
			reader, err := reader.Init(s.client, s.config.Type, s.config.Bucket, *obj.Key)
			if err != nil {
				return fmt.Errorf("failed to initialize reader on file[%s]: %s", *obj.Key, err)
			}
			schemas = append(schemas, reader.GetSchema())
		}
	}

	for i := 1; i < len(schemas); i++ {
		if !reflect.DeepEqual(schemas[i], schemas[i-1]) {
			return fmt.Errorf("different schemas across files")
		}
	}

	return nil
}

func (s *S3) Discover() ([]*kakumodels.Stream, error) {
	// List objects in the S3 bucket
	resp, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("Error listing objects: %s", err)
	}
	var schemas []map[string]*kakumodels.Property

	// iteration
	for resp.IsTruncated != nil && *resp.IsTruncated {
		for _, obj := range resp.Contents {
			if matcher.Matches(s.config.Pattern, *obj.Key, matcher.ValidateCase) {
				reader, err := reader.Init(s.client, s.config.Type, s.config.Bucket, *obj.Key)
				if err != nil {
					return nil, fmt.Errorf("failed to initialize reader on file[%s]: %s", *obj.Key, err)
				}
				schemas = append(schemas, reader.GetSchema())
			}
		}

		resp, err = s.client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(s.config.Bucket),
			ContinuationToken: resp.NextContinuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("Error listing objects: %s", err)
		}
	}

	// final
	for _, obj := range resp.Contents {
		if matcher.Matches(s.config.Pattern, *obj.Key, matcher.ValidateCase) {
			reader, err := reader.Init(s.client, s.config.Type, s.config.Bucket, *obj.Key)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize reader on file[%s]: %s", *obj.Key, err)
			}
			schemas = append(schemas, reader.GetSchema())
		}
	}

	if len(schemas) < 1 {
		return nil, fmt.Errorf("no schema found")
	}

	for i := 1; i < len(schemas); i++ {
		if !reflect.DeepEqual(schemas[i], schemas[i-1]) {
			return nil, fmt.Errorf("different schemas across files")
		}
	}

	return []*kakumodels.Stream{
		{
			Name:                    s.config.TargetStreamName,
			Namespace:               s.config.TargetStreamName,
			SupportedSyncModes:      []types.SyncMode{types.Incremental, types.FullRefresh},
			SourceDefinedCursor:     true,
			SourceDefinedPrimaryKey: []string{"last_modified_date"},
			JSONSchema: &kakumodels.Schema{
				Properties: schemas[0],
			},
		},
	}, nil
}

func (s *S3) Catalog() *kakumodels.Catalog {
	return s.catalog
}

func (s *S3) Type() string {
	return "S3"
}

func (s *S3) Read(stream protocol.Stream, channel chan<- kakumodels.Record) error {
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
			_, err := reader.Init(s.client, s.config.Type, s.config.Bucket, *obj.Key)
			if err != nil {
				return fmt.Errorf("failed to initialize reader on file[%s]: %s", *obj.Key, err)
			}

		}
	}

	return nil
}
func (s *S3) GetState() (*kakumodels.State, error) {
	return &s.state, nil
}
