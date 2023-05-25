package syndicate

import "github.com/spf13/cobra"

// DiscoverCmd represents the read command
var DiscoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "Syndicate discover command",
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}
