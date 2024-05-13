package protocol

import (
	"fmt"

	"github.com/piyushsingariya/shift/logger/console"
	"github.com/piyushsingariya/shift/types"
	"github.com/piyushsingariya/shift/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	config_    string
	state_     string
	catalog_   string
	batchSize_ uint64

	catalog *types.Catalog
	state   types.State
	config  any

	isDriver        = false
	driverCommands  = []*cobra.Command{}
	adapterCommands = []*cobra.Command{}

	_driver       Driver
	_adapter      Adapter
	_rawConnector Connector
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "shift",
	Short: "Shift is a data pipeline connectors",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// TODO: Add config check
		if catalog_ != "" {
			catalog = &types.Catalog{}
			if err := utils.Unmarshal(utils.ReadFile(catalog_), catalog); err != nil {
				return fmt.Errorf("failed to unmarshal catalog: %s", err)
			}
		}

		if state_ != "" {
			state = types.State{}
			err := utils.Unmarshal(utils.ReadFile(state_), &state)
			if err != nil {
				return fmt.Errorf("failed to unmarshal state file: %s", err)
			}
		}

		// if isDriver {
		// 	if err := _driver.Setup(utils.ReadFile(config_), base.NewDriver(catalog, state)); err != nil {
		// 		return err
		// 	}

		// 	return nil
		// }

		// return _adapter.Setup(utils.ReadFile(config_), base.NewDriver(catalog, state))
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}

		if ok := utils.IsValidSubcommand(getAvailableCommands(), args[0]); !ok {
			return fmt.Errorf("'%s' is an invalid command. Use 'shift --help' to display usage guide", args[0])
		}

		return nil
	},
}

func CreateRootCommand(forDriver bool, connector any) *cobra.Command {
	if forDriver {
		RootCmd.AddCommand(driverCommands...)
		_driver = connector.(Driver)
		isDriver = true
	} else {
		RootCmd.AddCommand(adapterCommands...)
		_adapter = connector.(Adapter)
	}

	_rawConnector = connector.(Connector)

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

	RootCmd.PersistentFlags().StringVarP(&config_, "config", "", "", "(Required) Config for Shift connector")
	RootCmd.PersistentFlags().StringVarP(&catalog_, "catalog", "", "", "(Required) Catalog for Shift connector")
	RootCmd.PersistentFlags().StringVarP(&state_, "state", "", "", "(Required) State for Shift connector")
	RootCmd.PersistentFlags().Uint64VarP(&batchSize_, "batch", "", 10000, "(Optional) Batch size for Shift connector")

	// Disable Cobra CLI's built-in usage and error handling
	RootCmd.SilenceUsage = true
	RootCmd.SilenceErrors = true

	// Disable logging
	logrus.SetOutput(nil)

	console.SetupWriter(RootCmd.OutOrStdout(), RootCmd.ErrOrStderr())
}
