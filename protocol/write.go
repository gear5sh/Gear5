package protocol

import "github.com/spf13/cobra"

// WriteCmd represents the read command
var WriteCmd = &cobra.Command{
	Use:   "write",
	Short: "Kaku write command",
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}
