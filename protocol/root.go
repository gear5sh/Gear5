package protocol

import (
	"fmt"

	"github.com/piyushsingariya/syndicate/logger"
	"github.com/piyushsingariya/syndicate/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	config    string
	state     string
	catalog   string
	batchSize int64

	isDriver        = false
	driverCommands  = []*cobra.Command{}
	adapterCommands = []*cobra.Command{}

	rawConnector interface{}
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "syndicate",
	Short: "Syndicate is a data pipeline connectors",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}

		if ok := utils.IsValidSubcommand(getAvailableCommands(), args[0]); !ok {
			return fmt.Errorf("'%s' is an invalid command. Use 'syndicate --help' to display usage guide", args[0])
		}

		return nil
	},
}

func CreateRootCommand(forDriver bool, connector interface{}) *cobra.Command {
	if forDriver {
		RootCmd.AddCommand(driverCommands...)
	} else {
		RootCmd.AddCommand(adapterCommands...)
	}

	rawConnector = connector

	return RootCmd
}

func getAvailableCommands() []*cobra.Command {
	if isDriver {
		return driverCommands
	}

	return adapterCommands
}

func init() {
	driverCommands = append(driverCommands, SpecCmd, CheckCmd, DiscoverCmd, ReadCmd)
	adapterCommands = append(adapterCommands, SpecCmd, CheckCmd, DiscoverCmd, WriteCmd)

	RootCmd.PersistentFlags().StringVarP(&config, "config", "", "", "(Required) Config for Syndicate connector")
	RootCmd.PersistentFlags().StringVarP(&catalog, "catalog", "", "", "(Required) Catalog for Syndicate connector")
	RootCmd.PersistentFlags().StringVarP(&state, "state", "", "", "(Required) State for Syndicate connector")
	RootCmd.PersistentFlags().Int64VarP(&batchSize, "batch", "", 1000, "(Optional) Batch size for Syndicate connector")

	// Disable Cobra CLI's built-in usage and error handling
	RootCmd.SilenceUsage = true
	RootCmd.SilenceErrors = true

	// Disable logging
	logrus.SetOutput(nil)

	logger.SetupWriter(RootCmd.OutOrStdout(), RootCmd.ErrOrStderr())
}
