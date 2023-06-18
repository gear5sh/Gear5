package main

import (
	"github.com/piyushsingariya/syndicate"
	"github.com/piyushsingariya/syndicate/drivers/hubspot/driver"
	"github.com/piyushsingariya/syndicate/logger"
)

func main() {
	// defer utils.Recovery()

	driver := &driver.Hubspot{}
	cmd, err := syndicate.RegisterDriver(driver)
	if err != nil {
		logger.Fatal(err)
	}

	// Execute the root command
	err = cmd.Execute()
	if err != nil {
		logger.Fatal(err)
	}
}
