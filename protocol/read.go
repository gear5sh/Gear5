package protocol

import (
	"fmt"
	"strings"
	"sync"

	"github.com/piyushsingariya/kaku/logger"
	"github.com/piyushsingariya/kaku/models"
	"github.com/piyushsingariya/kaku/utils"
	"github.com/spf13/cobra"
)

// ReadCmd represents the read command
var ReadCmd = &cobra.Command{
	Use:   "read",
	Short: "Kaku read command",
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
			return fmt.Errorf("expected type to be: Driver, found %T", connector)
		}

		if state == "" {
			if err := connector.Setup(utils.ReadFile(config), utils.ReadFile(catalog), nil, batchSize); err != nil {
				return err
			}
		} else {
			st := models.State{}
			err := utils.Unmarshal(utils.ReadFile(state), &st)
			if err != nil {
				return fmt.Errorf("failed to unmarshal state file")
			}
			if err := connector.Setup(utils.ReadFile(config), utils.ReadFile(catalog), st, batchSize); err != nil {
				return err
			}
		}

		waitgroup := sync.WaitGroup{}

		recordStream := make(chan models.Record, 2*batchSize)

		waitgroup.Add(1)
		go func() {
			defer func() {
				close(recordStream)
				waitgroup.Done()
			}()

			streamNames := []string{}
			for _, stream := range connector.Catalog().Streams {
				streamNames = append(streamNames, fmt.Sprintf("%s[%s]", stream.Name(), stream.Namespace()))
			}
			logger.Infof("Selected streams are %s", strings.Join(streamNames, " ,"))

			for _, stream := range connector.Catalog().Streams {
				logger.Info("Reading stream %s[%s]", stream.Name(), stream.Namespace())
				err := connector.Read(stream, recordStream)
				if err != nil {
					logger.Fatalf("Error occurred while reading recrods from [%s]: %s", connector.Type(), err)
				}
				logger.Info("Finished reading stream %s[%s]", stream.Name(), stream.Namespace())
			}
		}()

		numRecords := int64(0)
		batch := int64(0)
		for message := range recordStream {
			logger.LogRecord(message)
			numRecords++
			batch++

			if batch >= batchSize {
				state, err := connector.GetState()
				if err != nil {
					logger.Fatalf("failed to get state from connector")
				}

				logger.LogState(state)

				// reset batch
				batch = 0
			}
		}

		logger.Infof("Total records read: %d", numRecords)
		waitgroup.Wait()
		return nil
	},
}
