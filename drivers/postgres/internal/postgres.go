package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/piyushsingariya/shift/drivers/base"
	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/protocol"
	"github.com/piyushsingariya/shift/types"
	"github.com/piyushsingariya/shift/utils"
)

type Postgres struct {
	*base.Driver

	allStreams  map[string]*pgStream
	batchSize   int64
	client      *sqlx.DB
	accessToken string
	config      *Config
	// catalog     *types.Catalog
	// state       types.State
}

func (p *Postgres) Setup(config any, base *base.Driver) error {
	p.Driver = base

	cfg := Config{}
	err := utils.Unmarshal(config, &cfg)
	if err != nil {
		return err
	}

	err = cfg.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	db, err := sqlx.Open("pgx", cfg.ToConnectionString())
	if err != nil {
		return fmt.Errorf("failed to connect database: %s", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// force a connection and test that it worked
	err = db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping database: %s", err)
	}

	p.client = db.Unsafe()

	p.config = &cfg

	return p.setupStreams()
}

func (p *Postgres) CloseConnection() {
	if p.client != nil {
		err := p.client.Close()
		if err != nil {
			logger.Error("failed to close connection with postgres: %s", err)
		}
	}
}

func (p *Postgres) Spec() any {
	return Config{}
}

func (p *Postgres) Check() error {
	return nil
}

func (p *Postgres) Discover() ([]*types.Stream, error) {
	streams := []*types.Stream{}
	for _, stream := range p.allStreams {
		streams = append(streams, stream.Self().GetStream())
	}

	return streams, nil
}

func (p *Postgres) Type() string {
	return "Postgres"
}

func (p *Postgres) Streams() ([]*types.Stream, error) {
	return nil, nil
}

func (p *Postgres) Read(stream protocol.Stream, channel chan<- types.Record) error {
	identifier := utils.StreamIdentifier(stream.Namespace(), stream.Name())
	pgStream, found := p.allStreams[identifier]
	if !found {
		logger.Warnf("Stream %s.%s not found; skipping...", stream.Namespace(), stream.Name())
		return nil
	}

	switch stream.GetSyncMode() {
	case types.FULLREFRESH:
		return pgStream.readFullRefresh(p.client, channel)
	case types.INCREMENTAL:
		// read incrementally
		return pgStream.readIncremental(p.client, channel)
	}

	return nil
}

// func (p *Postgres) GetState() (*types.State, error) {
// 	state := &types.State{}
// 	for _, stream := range p.Catalog().Streams {
// 		if stream.SyncMode == types.Incremental || stream.SyncMode == types.CDC {
// 			pgStream, found := p.allStreams[utils.StreamIdentifier(stream.Namespace(), stream.Name())]
// 			if !found {
// 				return nil, fmt.Errorf("postgres stream not found while getting state of stream %s[%s]", stream.Name(), stream.Namespace())
// 			}

// 			if !(utils.ExistInArray(pgStream.SupportedSyncModes, types.Incremental) || utils.ExistInArray(pgStream.SupportedSyncModes, types.CDC)) {
// 				logger.Warnf("Skipping getting state from stream %s[%s], this stream doesn't support incremental/CDC", stream.Name(), stream.Namespace())
// 				continue
// 			}

// 			state.Add(stream.Name(), stream.Namespace(), map[string]any{
// 				pgStream.cursor: pgStream.state,
// 			})
// 		}
// 	}

// 	return state, nil
// }

func (p *Postgres) setupStreams() error {
	p.allStreams = make(map[string]*pgStream)

	var tableNamesOutput []Table
	err := p.client.Select(&tableNamesOutput, getPrivilegedTablesTmpl)
	if err != nil {
		return fmt.Errorf("failed to retrieve table names: %s", err)
	}

	if len(tableNamesOutput) == 0 {
		logger.Warnf("no tables found")
	}

	for _, table := range tableNamesOutput {
		var columnSchemaOutput []ColumnDetails
		err := p.client.Select(&columnSchemaOutput, getTableSchemaTmpl, table.Schema, table.Name)
		if err != nil {
			return fmt.Errorf("failed to retrieve column details for table %s[%s]: %s", table.Name, table.Schema, err)
		}

		if len(columnSchemaOutput) == 0 {
			logger.Warnf("no columns found in table %s[%s]", table.Name, table.Schema)
			continue
		}

		var primaryKeyOutput []ColumnDetails
		err = p.client.Select(&primaryKeyOutput, getTablePrimaryKey, table.Schema, table.Name)
		if err != nil {
			return fmt.Errorf("failed to retrieve primary key columns for table %s[%s]: %s", table.Name, table.Schema, err)
		}

		stream := &types.Stream{
			Name:      table.Name,
			Namespace: table.Schema,
		}

		for _, column := range columnSchemaOutput {
			datatype := types.UNKNOWN
			if val, found := pgTypeToDataTypes[*column.DataType]; found {
				datatype = val
			} else {
				logger.Warnf("failed to get respective type in datatypes for column: %s[%s]", column.Name, column.DataType)
			}

			stream.UpsertField(column.Name, datatype, strings.EqualFold("yes", *column.IsNullable))
		}

		stream.SupportedSyncModes = append(stream.SupportedSyncModes, types.FULLREFRESH)

		// currently only datetime fields is supported for cursor field, automatic generated fields can also be used
		// future TODO
		for propertyName, property := range stream.Schema.Properties {
			if utils.ExistInArray(property.Type, types.TIMESTAMP) {
				stream.DefaultCursorFields = append(stream.DefaultCursorFields, propertyName)
			}
		}

		// source has cursor fields, hence incremental also supported
		if len(stream.DefaultCursorFields) > 0 {
			stream.SourceDefinedCursor = true
			stream.SupportedSyncModes = append(stream.SupportedSyncModes, types.INCREMENTAL)
		}

		// add primary keys for stream
		for _, column := range primaryKeyOutput {
			stream.SourceDefinedPrimaryKey = append(stream.SourceDefinedPrimaryKey, column.Name)
		}

		p.allStreams[utils.StreamIdentifier(table.Schema, table.Name)] = &pgStream{}
	}

	return nil
}
