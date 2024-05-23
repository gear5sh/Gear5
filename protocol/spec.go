package protocol

import (
	"fmt"

	"github.com/piyushsingariya/shift/jsonschema"
	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/utils"

	"github.com/spf13/cobra"
)

var (
	generate    bool
	airbyteMode bool
)

// SpecCmd represents the read command
var SpecCmd = &cobra.Command{
	Use:   "spec",
	Short: "Shift spec command",
	RunE: func(cmd *cobra.Command, args []string) error {
		if generate {
			config := _rawConnector.Spec()
			schemaInMap := make(map[string]interface{})

			schema, err := jsonschema.Reflect(config)
			if err != nil {
				return err
			}

			err = utils.Unmarshal(schema, &schemaInMap)
			if err != nil {
				return fmt.Errorf("failed to generate json schema for config: %s", err)
			}

			logger.LogSpec(schemaInMap)
		}

		return nil
	},
}

func init() {
	// TODO: Set false
	RootCmd.PersistentFlags().BoolVarP(&generate, "generate", "", true, "(Optional) Generate Config")
	RootCmd.PersistentFlags().BoolVarP(&airbyteMode, "airbyte", "", false, "(Optional) Print Config wrapped like airbyte")
}
