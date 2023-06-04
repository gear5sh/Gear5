package protocol

import (
	"fmt"
	"sync"

	"github.com/piyushsingariya/syndicate/logger"
	"github.com/piyushsingariya/syndicate/models"
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

		streams, dataTypesProcessed, err := connector.Discover()
		if err != nil {
			logger.Fatal(err)
		}

		// incase datatypes are already processed by a connector we don't do processing
		if dataTypesProcessed {
			logger.LogCatalog(streams)
			return nil
		}

		err = connector.Setup(utils.ReadFile(config), models.GetWrappedCatalog(streams), nil, batchSize)
		if err != nil {
			logger.Fatal(err)
		}

		waitgroup := sync.WaitGroup{}

		for _, stream := range streams {
			recordStream := make(chan models.Record, 2*batchSize)

			waitgroup.Add(1)
			go func() {
				defer func() {
					close(recordStream)
					waitgroup.Done()
				}()

				err := connector.Read(stream, recordStream)
				if err != nil {
					logger.Fatalf("Error occurred while reading recrods from [%s]: %s", connector.Type(), err)
				}
			}()

			for message := range recordStream {
				utils.ProcessDataTypes(stream, message)
			}
		}

		waitgroup.Wait()

		logger.LogCatalog(streams)
		return nil
	},
}
