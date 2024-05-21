package types

import "github.com/apache/arrow/go/v16/arrow"

type DataType string

func (typ DataType) ToArrow() arrow.DataType {
	return dataTypeToArrow(typ)
}

const (
	NULL      DataType = "null"
	INT64     DataType = "integer"
	FLOAT64   DataType = "number"
	STRING    DataType = "string"
	BOOL      DataType = "boolean"
	OBJECT    DataType = "object"
	ARRAY     DataType = "array"
	UNKNOWN   DataType = "unknown"
	TIMESTAMP DataType = "timestamp"
)

type RecordData = map[string]any

func dataTypeToArrow(typ DataType) arrow.DataType {
	switch typ {
	case NULL:
		return arrow.Null
	case INT64:
		return arrow.PrimitiveTypes.Int64
	case FLOAT64:
		return arrow.PrimitiveTypes.Float64
	case STRING:
		return arrow.BinaryTypes.String
	case BOOL:
		return arrow.FixedWidthTypes.Boolean
	case OBJECT:
		return &arrow.MapType{}
	case ARRAY:
		return &arrow.ListType{}
	case TIMESTAMP:
		return arrow.FixedWidthTypes.Timestamp_ns
	default:
		return arrow.BinaryTypes.String
	}
}
