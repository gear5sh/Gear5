package protocol

import (
	"fmt"

	"github.com/piyushsingariya/syndicate/jsonschema"
	"github.com/piyushsingariya/syndicate/logger"
	"github.com/piyushsingariya/syndicate/utils"
	"github.com/spf13/cobra"
)

// SpecCmd represents the read command
var SpecCmd = &cobra.Command{
	Use:   "spec",
	Short: "Syndicate spec command",
	RunE: func(cmd *cobra.Command, args []string) error {
		connector, not := rawConnector.(Connector)
		if !not {
			return fmt.Errorf("expected type to be: Connector, found %T", connector)
		}

		schema, err := jsonschema.Reflect(connector.Spec())
		if err != nil {
			return fmt.Errorf("failed to generate json schema for config: %s", connector.Type())
		}

		schemaInMap := make(map[string]interface{})

		err = utils.Unmarshal(schema, &schemaInMap)
		if err != nil {
			return fmt.Errorf("failed to generate json schema for config: %s", connector.Type())
		}

		logger.LogSpec(schemaInMap)

		return nil
	},
}
