package main

import (
	"github.com/piyushsingariya/shift"
	driver "github.com/piyushsingariya/shift/drivers/hubspot/internal"
	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/safego"
)

func main() {
	defer safego.Recovery()

	driver := &driver.Hubspot{}
	cmd, err := shift.RegisterDriver(driver)
	if err != nil {
		logger.Fatal(err)
	}

	// Execute the root command
	err = cmd.Execute()
	if err != nil {
		logger.Fatal(err)
	}
}
