package main

import (
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/piyushsingariya/shift"
	"github.com/piyushsingariya/shift/drivers/base"
	driver "github.com/piyushsingariya/shift/drivers/postgres/internal"
)

func main() {
	driver := &driver.Postgres{
		Driver: base.NewBase(),
	}
	defer driver.CloseConnection()
	shift.RegisterDriver(driver)
}
