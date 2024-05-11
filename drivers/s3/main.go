package main

import (
	driver "github.com/piyushsingariya/drivers/s3/internal"
	"github.com/piyushsingariya/shift"
	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/safego"
)

func main() {
	defer safego.Recovery()

	driver := &driver.S3{}
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
