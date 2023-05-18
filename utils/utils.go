package utils

import (
	"github.com/piyushsingariya/syndicate/models"
	"github.com/spf13/cobra"
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
