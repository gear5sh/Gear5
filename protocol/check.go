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
		err := func() error {
			if config == "" {
				return fmt.Errorf("--config not passed")
			}

			if err := utils.CheckIfFilesExists(config_); err != nil {
				return err
			}

			if catalog_ != "" {
				return utils.CheckIfFilesExists(catalog_)
			}

			return nil
		}()
		if err != nil {
			logger.LogConnectionStatus(err)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		err := func() error {
			err := _rawConnector.Setup(utils.ReadFile(config_), base.NewDriver(nil, nil))
			if err != nil {
				return err
			}

			// TODO: Validate Streams
			// Check if the streams are valid

			return _rawConnector.Check()
		}()

		// success
		logger.LogConnectionStatus(err)
	},
}
