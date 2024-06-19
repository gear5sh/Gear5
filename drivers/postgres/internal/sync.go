package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gear5sh/gear5/drivers/base"
	"github.com/gear5sh/gear5/logger"
	"github.com/gear5sh/gear5/pkg/jdbc"
	"github.com/gear5sh/gear5/protocol"
	"github.com/gear5sh/gear5/safego"
	"github.com/gear5sh/gear5/types"
	"github.com/gear5sh/gear5/utils"
	"github.com/jmoiron/sqlx"
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

	setter := jdbc.NewReader(context.TODO(), stmt, int(stream.BatchSize()), func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return tx.Query(query, args...)
	})
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

	setter := jdbc.NewReader(context.Background(), statement, int(stream.BatchSize()), func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return tx.Query(query, args...)
	}, args...)
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
