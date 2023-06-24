package typing

import (
	"github.com/piyushsingariya/kaku/models"
)

func Resolve(objects ...map[string]interface{}) (map[string]*models.Property, error) {
	allfields := Fields{}

	for _, object := range objects {
		fields := Fields{}
		// apply default typecast and define column types
		for k, v := range object {
			fields[k] = NewField(TypeFromValue(v))
		}

		for fieldName, field := range allfields {
			if _, found := object[fieldName]; !found {
				field.setNullable()
			}
		}

		allfields.Merge(fields)
	}

	return allfields.ToProperties(), nil
}
