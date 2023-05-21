package connector

import (
	"fmt"

	"github.com/piyushsingariya/syndicate/utils"
	"github.com/spf13/cobra"
)

var (
	driverCommands       = []*cobra.Command{}
	availableSubcommands = []*cobra.Command{}
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "syndicate",
	Short: "syncd",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}

		if ok := utils.IsValidSubcommand(availableSubcommands, args[0]); !ok {
			return fmt.Errorf("'%s' is an invalid command. Use 'mesheryctl --help' to display usage guide", args[0])
		}

		return nil
	},
}

func CreateRootCommand(forDriver bool) {
	// ava
}
