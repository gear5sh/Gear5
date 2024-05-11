package main

import (
	driver "github.com/piyushsingariya/drivers/s3/internal"
	"github.com/piyushsingariya/shift"
)

func main() {
	driver := &driver.S3{}
	shift.RegisterDriver(driver)
}
