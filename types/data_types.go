package types

type DataType string

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
type Dict map[string]any

func (d Dict) Merge(other Dict) {
	for key, value := range other {
		d[key] = value
	}
}

func (d Dict) Add(key string, value any) Dict {
	d[key] = value

	return d
}
