package driver

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gobwas/glob"
	"github.com/piyushsingariya/drivers/s3/models"
	"github.com/piyushsingariya/drivers/s3/reader"
	"github.com/piyushsingariya/kaku/jsonschema"
	"github.com/piyushsingariya/kaku/jsonschema/schema"
	kakumodels "github.com/piyushsingariya/kaku/models"
	protocol "github.com/piyushsingariya/kaku/protocol"
	"github.com/piyushsingariya/kaku/types"
	"github.com/piyushsingariya/kaku/utils"
)

const patternSymbols = "*[]!{}"

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
	for stream, pattern := range s.config.Streams {
		err := s.iteration(pattern, func(reader reader.Reader) (bool, error) {
			// break iteration after single item
			return true, nil
		})
		if err != nil {
			return fmt.Errorf("failed to check stream[%s] pattern[%s]: %s", stream, pattern, err)
		}
	}

	return nil
}

func (s *S3) Discover() ([]*kakumodels.Stream, error) {
	streams := []*kakumodels.Stream{}
	for stream, pattern := range s.config.Streams {
		var schema map[string]*kakumodels.Property
		var err error
		err = s.iteration(pattern, func(reader reader.Reader) (bool, error) {
			schema, err = reader.GetSchema()
			return true, err
		})
		if err != nil {
			return nil, fmt.Errorf("failed to check stream[%s] pattern[%s]: %s", stream, pattern, err)
		}

		if schema == nil {
			return nil, fmt.Errorf("no schema found")
		}

		streams = append(streams, &kakumodels.Stream{
			Name:                stream,
			Namespace:           pattern,
			SupportedSyncModes:  []types.SyncMode{types.Incremental, types.FullRefresh},
			SourceDefinedCursor: true,
			DefaultCursorFields: []string{"last_modified_date"},
			JSONSchema: &kakumodels.Schema{
				Properties: schema,
			},
		})
	}

	return streams, nil
}

func (s *S3) Catalog() *kakumodels.Catalog {
	return s.catalog
}

func (s *S3) Type() string {
	return "S3"
}

func (s *S3) Read(stream protocol.Stream, channel chan<- kakumodels.Record) error {
	return nil
}

func (s *S3) GetState() (*kakumodels.State, error) {
	return &s.state, nil
}

func (s *S3) iteration(pattern string, foreach func(reader reader.Reader) (bool, error)) error {
	re, err := glob.Compile(pattern)
	if err != nil {
		return fmt.Errorf("failed to complie file pattern please check: https://github.com/gobwas/glob#performance")
	}

	var continuationToken *string
	prefix := ""
	split := strings.Split(pattern, "/")
	for _, i := range split {
		if strings.ContainsAny(i, patternSymbols) {
			break
		}
		prefix = filepath.Join(prefix, i)
	}

	// List objects in the S3 bucket
s3Iteration:
	for {
		resp, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(s.config.Bucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           aws.Int64(10000000),
			ContinuationToken: continuationToken, // Initialize with nil
		})
		if err != nil {
			return fmt.Errorf("Error listing objects: %s", err)
		}

		// Iterate through the objects and process them
		for _, obj := range resp.Contents {
			if re.Match(*obj.Key) {
				reader, err := reader.Init(s.client, s.config.Type, s.config.Bucket, *obj.Key)
				if err != nil {
					return fmt.Errorf("failed to initialize reader on file[%s]: %s", *obj.Key, err)
				}
				// execute foreach
				breakIteration, err := foreach(reader)
				if err != nil {
					return err
				}

				// break iteration
				if breakIteration {
					break s3Iteration
				}
			}
		}

		// Check if there are more objects to retrieve
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break // Break the loop if there are no more objects
		}

		// Update the continuation token for the next iteration
		continuationToken := resp.NextContinuationToken
		if continuationToken == nil {
			break // Break the loop if the continuation token is nil (should not happen)
		}
	}

	return nil
}
