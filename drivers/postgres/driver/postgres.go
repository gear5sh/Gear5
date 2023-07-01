package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/piyushsingariya/kaku/drivers/postgres/models"
	"github.com/piyushsingariya/kaku/jsonschema"
	"github.com/piyushsingariya/kaku/jsonschema/schema"
	"github.com/piyushsingariya/kaku/logger"
	kakumodels "github.com/piyushsingariya/kaku/models"
	"github.com/piyushsingariya/kaku/protocol"
	"github.com/piyushsingariya/kaku/types"
	"github.com/piyushsingariya/kaku/utils"
)

const (
	// get all schemas
	getSchemaNamesTmpl = `SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'`
	// get all tables from a schema
	getSchemaTablesTmpl = `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'`
	// get table schema
	getTableSchemaTmpl = `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`
	// get primary key columns
	getTablePrimaryKey = `SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`
)

type Postgres struct {
	allStreams  map[string]*kakumodels.Stream
	batchSize   int64
	client      *sqlx.DB
	accessToken string
	config      *models.Config
	catalog     *kakumodels.Catalog
	state       kakumodels.State
}

func (p *Postgres) Setup(config any, catalog *kakumodels.Catalog, state kakumodels.State, batchSize int64) error {
	cfg := models.Config{}
	err := utils.Unmarshal(config, &cfg)
	if err != nil {
		return err
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
	p.catalog = catalog
	p.batchSize = batchSize
	return nil
}

func (p *Postgres) CloseConnection() {
	if p.client != nil {
		err := p.client.Close()
		if err != nil {
			logger.Error("failed to close connection with postgres: %s", err)
		}
	}
}

func (p *Postgres) Spec() (schema.JSONSchema, error) {
	return jsonschema.Reflect(models.Config{})
}

func (p *Postgres) Check() error {
	return nil
}

func (p *Postgres) Discover() ([]*kakumodels.Stream, error) {
	streams := []*kakumodels.Stream{}
	for _, stream := range p.allStreams {
		streams = append(streams, stream)
	}

	return streams, nil
}

func (p *Postgres) Catalog() *kakumodels.Catalog {
	return nil
}
func (p *Postgres) Type() string {
	return "Postgres"
}

func (p *Postgres) Streams() ([]*kakumodels.Stream, error) {
	return nil, nil
}
func (p *Postgres) Read(stream protocol.Stream, channel chan<- kakumodels.Record) error {
	return nil
}

func (p *Postgres) GetState() (*kakumodels.State, error) {
	return nil, nil
}

func (p *Postgres) setupStreams() error {
	p.allStreams = make(map[string]*kakumodels.Stream)

	var schemaNamesOutput []models.Schema
	err := p.client.Select(&schemaNamesOutput, getSchemaNamesTmpl)
	if err != nil {
		return fmt.Errorf("failed to get schema names: %s", err)
	}

	if len(schemaNamesOutput) == 0 {
		return fmt.Errorf("no schemas found in database")
	}

	for _, schema := range schemaNamesOutput {
		var tableNamesOutput []models.Table
		err := p.client.Select(&tableNamesOutput, getSchemaTablesTmpl, schema.Name)
		if err != nil {
			return fmt.Errorf("failed to retrieve table names from schema[%s]: %s", schema.Name, err)
		}

		if len(tableNamesOutput) == 0 {
			logger.Warnf("no tables found in schema[%s]", schema.Name)
		}

		for _, table := range tableNamesOutput {
			var columnSchemaOutput []models.ColumnDetails
			err := p.client.Select(&columnSchemaOutput, getTableSchemaTmpl, schema.Name, table.Name)
			if err != nil {
				return fmt.Errorf("failed to retrieve column details for table %s[%s]: %s", table.Name, schema.Name, err)
			}

			if len(columnSchemaOutput) == 0 {
				logger.Warnf("no columns found in table %s[%s]", table.Name, schema.Name)
				continue
			}

			var primaryKeyOutput []models.ColumnDetails
			err = p.client.Select(&primaryKeyOutput, getTablePrimaryKey, schema.Name, table.Name)
			if err != nil {
				return fmt.Errorf("failed to retrieve primary key columns for table %s[%s]: %s", table.Name, schema.Name, err)
			}

			stream := &kakumodels.Stream{
				Name:      table.Name,
				Namespace: schema.Name,
			}

			stream.JSONSchema = &kakumodels.Schema{
				Properties: make(map[string]*kakumodels.Property),
			}

			for _, column := range columnSchemaOutput {
				datatypes := []types.DataType{}
				if val, found := pgTypeToDataTypes[*column.DataType]; found {
					datatypes = append(datatypes, val)
				} else {
					logger.Warnf("failed to get respective type in datatypes for column: %s[%s]", column.Name, column.DataType)
					datatypes = append(datatypes, types.UNKNOWN)
				}

				if column.IsNullable != nil && *column.IsNullable {
					datatypes = append(datatypes, types.NULL)
				}

				stream.JSONSchema.Properties[column.Name] = &kakumodels.Property{
					Type: datatypes,
				}
			}

			// add primary keys for stream
			for _, column := range primaryKeyOutput {
				stream.SourceDefinedPrimaryKey = append(stream.SourceDefinedPrimaryKey, column.Name)
			}

			p.allStreams[utils.StreamIdentifier(schema.Name, table.Name)] = stream
		}

	}

	return nil
}
