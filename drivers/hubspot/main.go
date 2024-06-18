package main

import (
	"github.com/piyushsingariya/synkit"
	driver "github.com/piyushsingariya/synkit/drivers/hubspot/internal"
)

func main() {
	driver := &driver.Hubspot{}
	synkit.RegisterDriver(driver)
}
