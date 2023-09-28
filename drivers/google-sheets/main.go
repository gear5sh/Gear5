package main

import (
	"github.com/piyushsingariya/shift"
	"github.com/piyushsingariya/shift/drivers/google-sheets/driver"
	"github.com/piyushsingariya/shift/logger"
)

func main() {
	driver := &driver.GoogleSheets{}
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
