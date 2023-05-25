package syndicate

import (
	"github.com/piyushsingariya/syndicate/utils"
	"github.com/spf13/cobra"
)

// ReadCmd represents the read command
var ReadCmd = &cobra.Command{
	Use:   "read",
	Short: "Syndicate read command",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return utils.CheckIfFilesExists(config, catalog)
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}
