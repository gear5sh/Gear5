package syndicate

import "github.com/spf13/cobra"

// WriteCmd represents the read command
var WriteCmd = &cobra.Command{
	Use:   "write",
	Short: "Syndicate write command",
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}
