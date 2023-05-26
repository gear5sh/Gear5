package main

import (
	"os"

	"github.com/piyushsingariya/syndicate"
	"github.com/piyushsingariya/syndicate/drivers/google-sheets/driver"
	"github.com/piyushsingariya/syndicate/logger"
)

func main() {
	driver := &driver.GoogleSheets{}
	cmd, err := syndicate.RegisterDriver(driver)
	if err != nil {
		logger.Fatal(err)
	}

	// Execute the root command
	err = cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
