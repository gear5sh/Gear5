package main

import (
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/piyushsingariya/shift"
	"github.com/piyushsingariya/shift/drivers/postgres/driver"
	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/safego"
)

func main() {
	defer safego.Recovery()

	driver := &driver.Postgres{}
	defer driver.CloseConnection()

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
