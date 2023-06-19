package protocol

import (
	"fmt"

	"github.com/piyushsingariya/syndicate/logger"
	"github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/safego"
	"github.com/piyushsingariya/syndicate/types"
	"github.com/piyushsingariya/syndicate/typing"
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

		recordsPerStream := 100

		for streamName, stream := range wrapForSchemaDiscovery(streams) {
			objects := []types.RecordData{}
			channel := make(chan models.Record, recordsPerStream)
			count := 0
			go func() {
				err := connector.Read(stream, channel)
				if err != nil {
					logger.Fatalf("Error occurred while reading records from [%s]: %s", streamName, err)
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

			properties, err := typing.Resolve(objects...)
			if err != nil {
				logger.Fatal(err)
			}

			stream.Stream.JSONSchema = &models.Schema{
				Properties: properties,
			}
		}

		logger.LogCatalog(streams)
		return nil
	},
}

func wrapForSchemaDiscovery(streams []*models.Stream) []*models.WrappedStream {
	wrappedStreams := []*models.WrappedStream{}

	for _, stream := range streams {
		// only adding streams for which json schema needs to be discovered
		if stream.JSONSchema == nil {
			wrappedStreams = append(wrappedStreams, &models.WrappedStream{
				SyncMode: types.FullRefresh,
				Stream:   stream,
			})
		}
	}

	return wrappedStreams
}
