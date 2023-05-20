package utils

import (
	"encoding/json"
	"fmt"

	"github.com/piyushsingariya/syndicate/jsonschema"
	"github.com/piyushsingariya/syndicate/models"
	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

// IsValidSubcommand checks if the passed subcommand is supported by the parent command
func IsValidSubcommand(available []*cobra.Command, sub string) bool {
	for _, s := range available {
		if sub == s.CalledAs() {
			return true
		}
	}
	return false
}

func GetStreamNamesFromConfiguredCatalog(catalog *models.ConfiguredCatalog) []string {
	result := []string{}
	for _, stream := range catalog.Streams {
		result = append(result, stream.Stream.Name)
	}

	return result
}

func ArrayContainsString(arr []string, str string) bool {
	for _, i := range arr {
		if i == str {
			return true
		}
	}

	return false
}

func ToJsonSchema(obj interface{}) (string, error) {
	schema, err := jsonschema.Reflect(obj)
	if err != nil {
		return "", err
	}

	j, err := json.MarshalIndent(schema, "", " ")
	if err != nil {
		return "", err
	}

	return string(j), nil
}

func ToYamlSchema(obj interface{}) (string, error) {
	jsonSchema, err := ToJsonSchema(obj)
	if err != nil {
		return "", err
	}

	yamlData, err := yaml.JSONToYAML([]byte(jsonSchema))
	if err != nil {
		return "", err
	}

	return string(yamlData), nil
}

// UnmarshalConfig serializes and deserializes config into the object
// return error if occurred
func UnmarshalConfig(config interface{}, object interface{}) error {
	reformatted := reformatInnerMaps(config)
	b, err := json.Marshal(reformatted)
	if err != nil {
		return fmt.Errorf("error marshalling object: %v", err)
	}
	err = json.Unmarshal(b, object)
	if err != nil {
		return fmt.Errorf("error unmarshalling config: %v", err)
	}

	return nil
}

// reformatInnerMaps converts all map[interface{}]interface{} into map[string]interface{}
// because json.Marshal doesn't support map[interface{}]interface{} (supports only string keys)
// but viper produces map[interface{}]interface{} for inner maps
// return recursively converted all map[interface]interface{} to map[string]interface{}
func reformatInnerMaps(valueI interface{}) interface{} {
	switch value := valueI.(type) {
	case []interface{}:
		for i, subValue := range value {
			value[i] = reformatInnerMaps(subValue)
		}
		return value
	case map[interface{}]interface{}:
		newMap := make(map[string]interface{}, len(value))
		for k, subValue := range value {
			newMap[fmt.Sprint(k)] = reformatInnerMaps(subValue)
		}
		return newMap
	case map[string]interface{}:
		for k, subValue := range value {
			value[k] = reformatInnerMaps(subValue)
		}
		return value
	default:
		return valueI
	}
}
