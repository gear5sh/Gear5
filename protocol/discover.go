package protocol

import (
	"sync"

	"github.com/piyushsingariya/shift/drivers/base"
	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/safego"
	"github.com/piyushsingariya/shift/types"
	"github.com/piyushsingariya/shift/typing"
	"github.com/piyushsingariya/shift/utils"
	"github.com/spf13/cobra"
)

// DiscoverCmd represents the read command
var DiscoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "Shift discover command",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return utils.CheckIfFilesExists(config_)
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		err := _driver.Setup(utils.ReadFile(config_), base.NewDriver())
		if err != nil {
			logger.Fatal(err)
		}

		streams, err := _driver.Discover()
		if err != nil {
			logger.Fatal(err)
		}

		if len(streams) == 0 {
			logger.Fatal("no streams found in connector")
		}

		recordsPerStream := 100
		group := sync.WaitGroup{}
		for _, stream_ := range streams {
			stream := stream_
			group.Add(1)

			go func() {
				objects := []types.RecordData{}
				channel := make(chan types.Record, recordsPerStream)
				count := 0
				go func() {
					err := _driver.Read(stream.Wrap(), channel)
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

				err := typing.Resolve(stream, objects...)
				if err != nil {
					logger.Fatal(err)
				}

				group.Done()
			}()
		}

		group.Wait()

		logger.LogCatalog(streams)
		return nil
	},
}
