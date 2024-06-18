package main

import (
	driver "github.com/piyushsingariya/drivers/s3/internal"
	"github.com/piyushsingariya/synkit"
)

func main() {
	driver := &driver.S3{}
	synkit.RegisterDriver(driver)
}
