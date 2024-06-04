package main

import (
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/piyushsingariya/shift"
	"github.com/piyushsingariya/shift/drivers/base"
	driver "github.com/piyushsingariya/shift/drivers/postgres/internal"
	"github.com/piyushsingariya/shift/protocol"
)

func main() {
	driver := &driver.Postgres{
		Driver: base.NewBase(),
	}
	_ = protocol.BulkDriver(driver)

	defer driver.CloseConnection()
	shift.RegisterDriver(driver)
}
