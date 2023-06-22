package main

import (
	"github.com/piyushsingariya/kaku"
	"github.com/piyushsingariya/kaku/drivers/google-sheets/driver"
	"github.com/piyushsingariya/kaku/logger"
)

func main() {
	driver := &driver.GoogleSheets{}
	cmd, err := kaku.RegisterDriver(driver)
	if err != nil {
		logger.Fatal(err)
	}

	// Execute the root command
	err = cmd.Execute()
	if err != nil {
		logger.Fatal(err)
	}
}
