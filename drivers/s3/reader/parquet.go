package reader

import (
	"context"

	_ "github.com/akrennmair/parquet-go-block-compressors/lz4raw" // registers the LZ4 block compressor with the LZ4_RAW compression type with parquet-go
	_ "github.com/akrennmair/parquet-go-brotli"                   // registers the Brotli block compressor with parquet-go

	// _ "github.com/akrennmair/parquet-go-lzo"                      // registers the LZO block compressor with parquet-go
	_ "github.com/akrennmair/parquet-go-zstd" // registers the Zstd block compressor with parquet-go
	kakumodels "github.com/piyushsingariya/kaku/models"
	"github.com/piyushsingariya/kaku/types"

	"github.com/aws/aws-sdk-go/service/s3"
	goparquet "github.com/fraugster/parquet-go"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
)

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

func (p *Parquet) GetSchema() map[string]*kakumodels.Property {
	output := make(map[string]*kakumodels.Property)
	for _, column := range p.reader.Columns() {
		output[column.Name()] = &kakumodels.Property{
			Type: []types.DataType{types.DataType(column.Type().String())},
		}
	}

	return output
}
