package main

import (
	"github.com/piyushsingariya/syndicate"
	"github.com/piyushsingariya/syndicate/drivers/hubspot/driver"
	"github.com/piyushsingariya/syndicate/logger"
	"github.com/piyushsingariya/syndicate/safego"
)

func main() {
	defer safego.Recovery()

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
