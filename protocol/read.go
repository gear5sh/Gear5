package protocol

import (
	"strings"
	"sync"
	"time"

	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/types"
	"github.com/piyushsingariya/shift/utils"
	"github.com/spf13/cobra"
)

// ReadCmd represents the read command
var ReadCmd = &cobra.Command{
	Use:   "read",
	Short: "Shift read command",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if err := utils.CheckIfFilesExists(config_, catalog_); err != nil {
			return err
		}

		if state_ != "" {
			return utils.CheckIfFilesExists(state_)
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		recordStream := make(chan types.Record, 2*batchSize_)
		numRecords := int64(0)
		batch := uint64(0)
		recordIterationWait := sync.WaitGroup{}

		recordIterationWait.Add(1)
		go func() {
			defer recordIterationWait.Done()
			defer close(recordStream)

			for message := range recordStream {
				// close the iteration
				if message.Close {
					break
				}

				logger.LogRecord(message)
				numRecords++
				batch++

				// log state after a batch
				if batch >= batchSize_ {
					if !state.IsZero() {
						logger.LogState(state)
					}
					// reset batch
					batch = 0
				}
			}
		}()

		selectedStreams := []string{}
		validStreams := []Stream{}
		_, _ = utils.ArrayContains(catalog.Streams, func(elem *types.ConfiguredStream) bool {
			err := elem.SetupState(state)
			if err != types.StateValid {
				logger.Errorf("Skipping stream %s due to reason: %s", elem.ID(), err)
				return false
			}

			selectedStreams = append(selectedStreams, elem.ID())
			validStreams = append(validStreams, elem)
			return false
		})
		logger.Infof("Valid selected streams are %s", strings.Join(selectedStreams, ", "))

		for _, stream := range catalog.Streams {
			if stream.Namespace() != "" {
				logger.Infof("Reading stream %s[%s]", stream.Name(), stream.Namespace())
			} else {
				logger.Infof("Reading stream %s", stream.Name())
			}

			streamStartTime := time.Now()
			err := _driver.Read(stream, recordStream)
			if err != nil {
				logger.Fatalf("Error occurred while reading records from [%s]: %s", _driver.Type(), err)
			}

			logger.Infof("Finished reading stream %s[%s] in %s", stream.Name(), stream.Namespace(), time.Since(streamStartTime).String())
		}

		// stop record iteration
		recordStream <- types.Record{
			Close: true,
		}
		recordIterationWait.Wait()

		logger.Infof("Total records read: %d", numRecords)
		if !state.IsZero() {
			logger.LogState(state)
		}

		return nil
	},
}
