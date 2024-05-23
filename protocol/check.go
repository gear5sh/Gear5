package protocol

import (
	"fmt"

	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/types"
	"github.com/piyushsingariya/shift/utils"
	"github.com/spf13/cobra"
)

// SpecCmd represents the read command
var CheckCmd = &cobra.Command{
	Use:   "check",
	Short: "Shift spec command",
	PreRun: func(cmd *cobra.Command, args []string) {
		err := func() error {
			if config_ == "" {
				return fmt.Errorf("--config not passed")
			} else {
				if err := utils.UnmarshalFile(config_, _rawConnector.Config()); err != nil {
					return err
				}
			}

			if catalog_ != "" {
				catalog = &types.Catalog{}
				if err := utils.UnmarshalFile(catalog_, &catalog); err != nil {
					return err
				}
			}

			return nil
		}()
		if err != nil {
			logger.LogConnectionStatus(err)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		err := func() error {
			// setup base
			if isDriver {
				_driver.SetupBase()
			}

			// Catalog has been passed setup and is driver; Connector should be setup
			if isDriver && catalog != nil {
				err := _rawConnector.Setup()
				if err != nil {
					return err
				}

				// Get Source Streams
				streams, err := _driver.Discover()
				if err != nil {
					return err
				}

				streamsMap := types.StreamsToMap(streams...)

				// Validating Streams
				invalidStreams := []string{}
				missingStreams := []string{}
				_, _ = utils.ArrayContains(catalog.Streams, func(stream *types.ConfiguredStream) bool {
					source, found := streamsMap[stream.ID()]
					if !found {
						missingStreams = append(missingStreams, stream.ID())
						return false
					}

					err := stream.Validate(source)
					if err != nil {
						invalidStreams = append(invalidStreams, stream.ID())
					}

					return false
				})

				if len(invalidStreams) > 0 && len(missingStreams) > 0 {
					return fmt.Errorf("found missing streams: %v and invalid streams: %v", missingStreams, invalidStreams)
				} else if len(invalidStreams) > 0 {
					return fmt.Errorf("found invalid streams: %v", invalidStreams)
				} else if len(missingStreams) > 0 {
					return fmt.Errorf("found missing streams: %v", missingStreams)
				}
			} else {
				// Only perform checks
				err := _rawConnector.Check()
				if err != nil {
					return err
				}
			}

			return nil
		}()

		// success
		logger.LogConnectionStatus(err)
	},
}
