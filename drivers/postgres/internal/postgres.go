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

	batchSize   int64
	client      *sqlx.DB
	accessToken string
	config      *Config // postgres driver connection config
	cdcEnabled  bool
	cdcConfig   CDC
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

	found, _ := utils.IsOfType(cfg.UpdateMethod, "replication_slot")
	if found {
		logger.Info("Found CDC Configuration")
		cdc := &CDC{}
		if err := utils.Unmarshal(cfg.UpdateMethod, cdc); err != nil {
			return err
		}

		p.cdcEnabled = true
		p.cdcConfig = *cdc
	} else {
		logger.Info("Standard Replication is selected")
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

	return p.loadStreams()
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
	for _, stream := range p.SourceStreams {
		streams = append(streams, stream)
	}

	return streams, nil
}

func (p *Postgres) Type() string {
	return "Postgres"
}

func (p *Postgres) Read(stream protocol.Stream, channel chan<- types.Record) error {
	switch stream.GetSyncMode() {
	case types.FULLREFRESH:
		return freshSync(p.client, stream, channel)
	case types.INCREMENTAL:
		// read incrementally
		return incrementalSync(p.client, stream, channel)
	}

	return nil
}

func (p *Postgres) GroupRead(channel chan<- types.Record, streams ...protocol.Stream) error {

	return nil
}

func (p *Postgres) loadStreams() error {
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

		// create new stream
		stream := types.NewStream(table.Name, table.Schema)

		for _, column := range columnSchemaOutput {
			datatype := types.UNKNOWN
			if val, found := pgTypeToDataTypes[*column.DataType]; found {
				datatype = val
			} else {
				logger.Warnf("failed to get respective type in datatypes for column: %s[%s]", column.Name, column.DataType)
			}

			stream.UpsertField(column.Name, datatype, strings.EqualFold("yes", *column.IsNullable))
		}

		// currently only datetime fields is supported for cursor field, automatic generated fields can also be used
		// future TODO
		for propertyName, property := range stream.Schema.Properties {
			if utils.ExistInArray(property.Type, types.TIMESTAMP) {
				stream.WithCursorField(propertyName)
			}
		}

		if !p.cdcEnabled {
			stream.WithSyncMode(types.FULLREFRESH)
			// source has cursor fields, hence incremental also supported
			if stream.DefaultCursorFields.Len() > 0 {
				stream.WithSyncMode(types.INCREMENTAL)
			}
		} else {
			stream.WithSyncMode(types.CDC)
		}

		// add primary keys for stream
		for _, column := range primaryKeyOutput {
			stream.WithPrimaryKey(column.Name)
		}

		// Cache it
		p.SourceStreams[utils.StreamIdentifier(table.Schema, table.Name)] = stream
	}

	return nil
}
