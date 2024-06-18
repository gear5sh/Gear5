package main

import (
	"github.com/piyushsingariya/synkit"
	driver "github.com/piyushsingariya/synkit/drivers/google-sheets/internal"
)

func main() {
	driver := &driver.GoogleSheets{}
	synkit.RegisterDriver(driver)
}
