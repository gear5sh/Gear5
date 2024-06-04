package main

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var releaserCMD = &cobra.Command{
	Use: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

func main() {
	err := releaserCMD.Execute()
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
}
