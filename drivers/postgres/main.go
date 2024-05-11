package main

import (
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/piyushsingariya/shift"
	driver "github.com/piyushsingariya/shift/drivers/postgres/internal"
)

func main() {
	driver := &driver.Postgres{}
	defer driver.CloseConnection()
	shift.RegisterDriver(driver)
}
