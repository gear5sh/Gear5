package schemas

import "github.com/apache/arrow/go/v16/arrow"

type DataTableSchema struct {
	TableName string
	Schema    *arrow.Schema
}
