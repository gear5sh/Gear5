package reader

import (
	"context"
	"fmt"

	_ "github.com/akrennmair/parquet-go-block-compressors/lz4raw" // registers the LZ4 block compressor with the LZ4_RAW compression type with parquet-go
	_ "github.com/akrennmair/parquet-go-brotli"                   // registers the Brotli block compressor with parquet-go

	// _ "github.com/akrennmair/parquet-go-lzo"                      // registers the LZO block compressor with parquet-go
	_ "github.com/akrennmair/parquet-go-zstd" // registers the Zstd block compressor with parquet-go
	kakumodels "github.com/piyushsingariya/kaku/models"
	"github.com/piyushsingariya/kaku/types"
	"github.com/piyushsingariya/kaku/utils"

	"github.com/aws/aws-sdk-go/service/s3"
	goparquet "github.com/fraugster/parquet-go"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
)

// ParquetType represents the data types used in Parquet.
type ParquetType struct {
	DataType     types.DataType
	ParquetTypes []string
}

// ParquetTypes defines all possible Parquet data types.
var ParquetTypes = map[string]ParquetType{
	"string": {
		DataType:     types.STRING,
		ParquetTypes: []string{"BYTE_ARRAY"},
	},
	"boolean": {
		DataType:     types.BOOL,
		ParquetTypes: []string{"BOOLEAN"},
	},
	"number": {
		DataType:     types.FLOAT64,
		ParquetTypes: []string{"DOUBLE", "FLOAT"},
	},
	"integer": {
		DataType:     types.INT64,
		ParquetTypes: []string{"INT32", "INT64", "INT96"},
	},
	"decimal": {
		DataType:     types.FLOAT64,
		ParquetTypes: []string{"INT32", "INT64", "FIXED_LEN_BYTE_ARRAY"},
	},
	"timestamp": {
		DataType:     types.TIMESTAMP,
		ParquetTypes: []string{"INT32", "INT64", "INT96"},
	},
	"date": {
		DataType:     types.TIMESTAMP,
		ParquetTypes: []string{"INT32", "INT64", "INT96"},
	},
	"time": {
		DataType:     types.TIMESTAMP,
		ParquetTypes: []string{"INT32", "INT64", "INT96"},
	},
}

type Parquet struct {
	reader *goparquet.FileReader
}

func InitParquet(s3Session *s3.S3, bucket, fileKey string) (*Parquet, error) {
	source, err := s3parquet.NewS3FileReaderWithClient(context.Background(), s3Session, bucket, fileKey)
	if err != nil {
		return nil, err
	}

	reader, err := goparquet.NewFileReader(source)
	if err != nil {
		return nil, err
	}

	return &Parquet{
		reader: reader,
	}, nil
}

func (p *Parquet) parseFieldType(neededLogicalType, neededPQType string) (types.DataType, error) {
	if pqType, found := ParquetTypes[neededLogicalType]; found {
		if neededPQType != "" && utils.ArrayContains(pqType.ParquetTypes, neededPQType) {
			return "", fmt.Errorf("incorrect parquet physical type[%s]; logical type[%s]", neededPQType, neededLogicalType)
		}

		return pqType.DataType, nil
	}

	for _, pqType := range ParquetTypes {
		if utils.ArrayContains(pqType.ParquetTypes, neededPQType) {
			return pqType.DataType, nil
		}
	}

	return "", fmt.Errorf("incorrect parquet physical type[%s]; logical type[%s]", neededPQType, neededLogicalType)
}

func (p *Parquet) GetSchema() (map[string]*kakumodels.Property, error) {
	output := make(map[string]*kakumodels.Property)
	for _, column := range p.reader.Columns() {

		columnType, err := p.parseFieldType(column.Element().GetLogicalType().String(), column.Type().String())
		if err != nil {
			return nil, err
		}

		output[column.Name()] = &kakumodels.Property{
			Type: []types.DataType{columnType},
		}
	}

	return output, nil
}
