package main

import (
	"github.com/piyushsingariya/shift"
	driver "github.com/piyushsingariya/shift/drivers/hubspot/internal"
)

func main() {
	driver := &driver.Hubspot{}
	shift.RegisterDriver(driver)
}
