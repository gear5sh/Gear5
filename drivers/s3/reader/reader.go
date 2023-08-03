package reader

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	kakumodels "github.com/piyushsingariya/kaku/models"
)

var FileTypes = []string{"parquet"}

type Reader interface {
	GetSchema() map[string]*kakumodels.Property
}

func Init(s3 *s3.S3, _type, bucket, file string) (Reader, error) {
	switch strings.ToLower(_type) {
	case "parquet":
		return InitParquet(s3, bucket, file)
	default:
		return nil, fmt.Errorf("reader not available to file format: %s", _type)
	}
}
