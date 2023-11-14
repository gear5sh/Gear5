package models

type Table struct {
	Schema string `db:"table_schema"`
	Name   string `db:"table_name"`
}

type ColumnDetails struct {
	Name       string  `db:"column_name"`
	DataType   *string `db:"data_type"`
	IsNullable *string `db:"is_nullable"`
}
