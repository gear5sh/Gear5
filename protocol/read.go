package protocol

import (
	"fmt"
	"sync"

	"github.com/piyushsingariya/syndicate/logger"
	"github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/utils"
	"github.com/spf13/cobra"
)

// ReadCmd represents the read command
var ReadCmd = &cobra.Command{
	Use:   "read",
	Short: "Syndicate read command",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if err := utils.CheckIfFilesExists(config, catalog); err != nil {
			return err
		}

		if state != "" {
			return utils.CheckIfFilesExists(state)
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		connector, not := rawConnector.(Driver)
		if !not {
			return fmt.Errorf("expected type to be: Connector, found %T", connector)
		}

		if state == "" {
			if err := connector.Setup(utils.ReadFile(config), nil, utils.ReadFile(catalog), batchSize); err != nil {
				return err
			}
		} else {
			if err := connector.Setup(utils.ReadFile(config), utils.ReadFile(state), utils.ReadFile(catalog), batchSize); err != nil {
				return err
			}
		}

		waitgroup := sync.WaitGroup{}

		recordStream := make(chan models.RecordRow, 2*batchSize)

		waitgroup.Add(1)
		go func() {
			defer func() {
				close(recordStream)
				waitgroup.Done()
			}()

			selectedStreams := utils.GetStreamNamesFromConfiguredCatalog(connector.Catalog())
			logger.Infof("Selected streams are %v", selectedStreams)
			for _, streamName := range selectedStreams {
				err := connector.Read(streamName, recordStream)
				if err != nil {
					logger.Fatalf("Error occurred while reading recrods from [%s]: %s", connector.Type(), err)
				}
			}
		}()

		numRecords := int64(0)
		for message := range recordStream {
			logger.LogRecord(message)
			numRecords++
		}

		logger.Infof("Total records read: %d", numRecords)
		waitgroup.Wait()
		return nil
	},
}
