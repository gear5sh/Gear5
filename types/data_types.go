package types

type DataType string

const (
	Null    DataType = "null"
	Integer DataType = "integer"
	Number  DataType = "number"
	String  DataType = "string"
	Boolean DataType = "boolean"
	Object  DataType = "object"
	Array   DataType = "array"
)
