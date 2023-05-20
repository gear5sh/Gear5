package utils

import (
	"encoding/json"

	"github.com/invopop/jsonschema"
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
	reflector := new(jsonschema.Reflector)
	reflector.AllowAdditionalProperties = true
	reflector.RequiredFromJSONSchemaTags = true
	reflector.ExpandedStruct = true
	schema := reflector.Reflect(obj)

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
