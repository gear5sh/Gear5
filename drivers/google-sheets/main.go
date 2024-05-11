package main

import (
	"github.com/piyushsingariya/shift"
	driver "github.com/piyushsingariya/shift/drivers/google-sheets/internal"
)

func main() {
	driver := &driver.GoogleSheets{}
	shift.RegisterDriver(driver)
}
