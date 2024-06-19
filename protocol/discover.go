package protocol

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gear5sh/gear5/logger"
	"github.com/gear5sh/gear5/safego"
	"github.com/gear5sh/gear5/types"
	"github.com/gear5sh/gear5/typeutils"
	"github.com/gear5sh/gear5/utils"
	"github.com/spf13/cobra"
)

// DiscoverCmd represents the read command
var DiscoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "discover command",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if config_ == "" {
			return fmt.Errorf("--config not passed")
		} else {
			if err := utils.UnmarshalFile(config_, _rawConnector.Config()); err != nil {
				return err
			}
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		err := _driver.Setup()
		if err != nil {
			return err
		}

		streams, err := _driver.Discover()
		if err != nil {
			return err
		}

		if len(streams) == 0 {
			return errors.New("no streams found in connector")
		}

		recordsPerStream := 100
		group := sync.WaitGroup{}
		for _, stream_ := range streams {
			if stream_.Schema != nil {
				continue
			}

			logger.Infof("Generating Type Schema for stream: %s", stream_.ID())

			stream := stream_
			group.Add(1)

			go func() {
				objects := []types.RecordData{}
				channel := make(chan types.Record, recordsPerStream)
				count := 0
				go func() {
					err := _driver.Read(stream.Wrap(recordsPerStream), channel)
					if err != nil {
						logger.Fatalf("Error occurred while reading records from [%s]: %s", stream.Name, err)
					}

					// close channel incase records are less than recordsPerStream
					safego.Close(channel)
				}()

				for record := range channel {
					count++
					objects = append(objects, record.Data)
					if count >= recordsPerStream {
						safego.Close(channel)
					}
				}

				err := typeutils.Resolve(stream, objects...)
				if err != nil {
					logger.Fatal(err)
				}

				logger.Infof("Type Schema generated for stream: %s", stream.ID())
				group.Done()
			}()
		}

		group.Wait()

		logger.LogCatalog(streams)
		return nil
	},
}
