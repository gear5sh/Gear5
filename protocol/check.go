package protocol

import (
	"fmt"

	"github.com/piyushsingariya/shift/drivers/base"
	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/utils"
	"github.com/spf13/cobra"
)

// SpecCmd represents the read command
var CheckCmd = &cobra.Command{
	Use:   "check",
	Short: "Shift spec command",
	PreRun: func(cmd *cobra.Command, args []string) {
		if config == "" {
			logger.LogConnectionStatus(fmt.Errorf("--config not passed"))
		}

		if err := utils.CheckIfFilesExists(config); err != nil {
			logger.LogConnectionStatus(err)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		connector, not := rawConnector.(Connector)
		if !not {
			logger.LogConnectionStatus(fmt.Errorf("expected type to be: Connector, found %T", connector))
		}

		err := connector.Setup(utils.ReadFile(config), base.NewDriver(nil, nil, batchSize))
		if err != nil {
			logger.LogConnectionStatus(err)
		}

		err = connector.Check()
		if err != nil {
			logger.LogConnectionStatus(err)
		}

		// success
		logger.LogConnectionStatus(nil)
	},
}
