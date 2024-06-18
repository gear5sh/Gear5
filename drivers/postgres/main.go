package main

import (
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/piyushsingariya/synkit"
	"github.com/piyushsingariya/synkit/drivers/base"
	driver "github.com/piyushsingariya/synkit/drivers/postgres/internal"
	"github.com/piyushsingariya/synkit/protocol"
)

func main() {
	driver := &driver.Postgres{
		Driver: base.NewBase(),
	}
	_ = protocol.BulkDriver(driver)

	defer driver.CloseConnection()
	synkit.RegisterDriver(driver)
}
