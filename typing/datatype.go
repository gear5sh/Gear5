package typing

import (
	"reflect"

	"github.com/piyushsingariya/syndicate/types"
)

// TypeFromValue return DataType from v type
func TypeFromValue(v interface{}) types.DataType {
	switch reflect.TypeOf(v).Kind() {
	case reflect.Invalid:
		return types.NULL
	case reflect.Bool:
		return types.BOOL
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return types.INT64
	case reflect.Float32, reflect.Float64:
		return types.FLOAT64
	case reflect.String:
		// Check for specific formats
		if value, ok := v.(string); ok {
			// Check for email format
			if _, err := ReformatDate(value); err != nil {
				return types.TIMESTAMP
			}
		}

		return types.STRING
	case reflect.Slice, reflect.Array:
		return types.ARRAY
	case reflect.Map:
		return types.OBJECT
	default:
		return types.UNKNOWN
	}
}
