package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/piyushsingariya/shift/drivers/base"
	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/pkg/jdbc"
	"github.com/piyushsingariya/shift/protocol"
	"github.com/piyushsingariya/shift/safego"
	"github.com/piyushsingariya/shift/types"
	"github.com/piyushsingariya/shift/utils"
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

	stmt := jdbc.PostgresFullRefresh(stream)

	setter := jdbc.NewOffsetter(stmt, int(stream.BatchSize()), tx.Query)
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
func (p *Postgres) incrementalSync(stream protocol.Stream, channel chan<- types.Record) error {
	tx, err := p.client.BeginTx(context.TODO(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return err
	}

	defer tx.Rollback()

	intialState := stream.InitialState()
	args := []any{}
	statement := jdbc.PostgresWithoutState(stream)
	if intialState != nil {
		logger.Debugf("Using Initial state for stream %s : %v", stream.ID(), intialState)
		statement = jdbc.PostgresWithState(stream)
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

		// insert record
		if !safego.Insert(channel, base.ReformatRecord(stream, record)) {
			// channel was closed
			return nil
		}

		err = p.UpdateState(stream, record)
		if err != nil {
			return err
		}

		return nil
	})
}
