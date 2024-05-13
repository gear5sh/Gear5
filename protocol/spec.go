package protocol

import (
	"fmt"

	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/utils"
	"github.com/spf13/cobra"
)

// SpecCmd represents the read command
var SpecCmd = &cobra.Command{
	Use:   "spec",
	Short: "Shift spec command",
	RunE: func(cmd *cobra.Command, args []string) error {
		schema, err := _rawConnector.Spec()
		if err != nil {
			return fmt.Errorf("failed to generate json schema: %s", err)
		}

		schemaInMap := make(map[string]interface{})

		err = utils.Unmarshal(schema, &schemaInMap)
		if err != nil {
			return fmt.Errorf("failed to generate json schema for config: %s", err)
		}

		logger.LogSpec(schemaInMap)

		return nil
	},
}
