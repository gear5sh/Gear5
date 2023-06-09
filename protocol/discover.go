package protocol

import (
	"fmt"

	"github.com/piyushsingariya/syndicate/logger"
	"github.com/piyushsingariya/syndicate/utils"
	"github.com/spf13/cobra"
)

// DiscoverCmd represents the read command
var DiscoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "Syndicate discover command",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return utils.CheckIfFilesExists(config)
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		connector, not := rawConnector.(Driver)
		if !not {
			logger.Fatal(fmt.Errorf("expected type to be: Connector, found %T", connector))
		}

		err := connector.Setup(utils.ReadFile(config), nil, nil, batchSize)
		if err != nil {
			logger.Fatal(err)
		}

		err = connector.Check()
		if err != nil {
			logger.Fatal(err)
		}

		streams, err := connector.Discover()
		if err != nil {
			logger.Fatal(err)
		}

		logger.LogCatalog(streams)
		return nil
	},
}
