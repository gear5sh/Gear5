package driver

import (
	"github.com/jmoiron/sqlx"
	"github.com/piyushsingariya/shift/protocol"
	"github.com/piyushsingariya/shift/types"
)

// Write Ahead Log Sync
func walSync(client *sqlx.DB, stream protocol.Stream, channel chan<- types.Record) error {
	return nil
}
