package main

import (
	"github.com/piyushsingariya/drivers/s3/driver"
	"github.com/piyushsingariya/kaku"
	"github.com/piyushsingariya/kaku/logger"
	"github.com/piyushsingariya/kaku/safego"
)

func main() {
	defer safego.Recovery()

	driver := &driver.S3{}
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
