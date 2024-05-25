package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/piyushsingariya/shift/drivers/base"
	"github.com/piyushsingariya/shift/pkg/jdbc"
	"github.com/piyushsingariya/shift/protocol"
	"github.com/piyushsingariya/shift/safego"
	"github.com/piyushsingariya/shift/types"
	"github.com/piyushsingariya/shift/typing"
	"github.com/piyushsingariya/shift/utils"
)

const (
	fullRefreshTemplate  = `SELECT * FROM "%s"."%s" ORDER BY %s`
	withStateTemplate    = `SELECT * FROM "%s"."%s" where "%s">$1 ORDER BY "%s" ASC NULLS FIRST`
	withoutStateTemplate = `SELECT * FROM "%s"."%s" ORDER BY "%s" ASC NULLS FIRST`
)

// Simple Full Refresh Sync; Loads table fully
func freshSync(client *sqlx.DB, stream protocol.Stream, channel chan<- types.Record) error {
	tx, err := client.BeginTx(context.TODO(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return err
	}

	defer tx.Rollback()

	setter := jdbc.NewOffsetter(fullRefreshTemplate, int(stream.BatchSize()), tx.Query)
	return setter.Capture(func(rows *sql.Rows) error {
		// Create a map to hold column names and values
		record := make(types.RecordData)

		// Scan the row into the map
		err := utils.MapScan(rows, record)
		if err != nil {
			return fmt.Errorf("failed to mapScan record data: %s", err)
		}

		// insert record
		if !safego.Insert(channel, base.ReformatRecord(stream, record)) {
			// channel was closed
			return nil
		}

		return nil
	})
}

// Incremental Sync based on a Cursor Value
func incrementalSync(client *sqlx.DB, stream protocol.Stream, channel chan<- types.Record) error {
	intialState := stream.InitialState()

	tx, err := client.BeginTx(context.TODO(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return err
	}

	defer tx.Rollback()
	cursorDataType, err := stream.Schema().GetType(stream.Cursor())
	if err != nil {
		return err
	}

	args := []any{}
	statement := fmt.Sprintf(withoutStateTemplate, stream.Namespace(), stream.Name(), stream.Cursor())
	if intialState != nil {
		statement = fmt.Sprintf(withStateTemplate, stream.Namespace(), stream.Name(), stream.Cursor(), stream.Cursor())
		args = append(args, intialState)
	}

	setter := jdbc.NewOffsetter(statement, int(stream.BatchSize()), tx.Query, args...)
	return setter.Capture(func(rows *sql.Rows) error {
		// Create a map to hold column names and values
		record := make(types.RecordData)

		// Scan the row into the map
		err := utils.MapScan(rows, record)
		if err != nil {
			return fmt.Errorf("failed to mapScan record data: %s", err)
		}

		if cursorVal, found := record[stream.Cursor()]; found && cursorVal != nil {
			// compare with current state
			if stream.GetState() != nil {
				state, err := typing.MaximumOnDataType(cursorDataType, stream.GetState(), cursorVal)
				if err != nil {
					return err
				}

				stream.SetState(state)
			} else {
				// directly update
				stream.SetState(cursorVal)
			}
		}

		// insert record
		if !safego.Insert(channel, base.ReformatRecord(stream, record)) {
			// channel was closed
			return nil
		}

		return nil
	})
}
