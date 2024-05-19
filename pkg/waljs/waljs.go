package waljs

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/piyushsingariya/shift/pkg/waljs/internal/helpers"
	"github.com/piyushsingariya/shift/pkg/waljs/internal/schemas"
	"github.com/sirupsen/logrus"
)

var pluginArguments = []string{"\"pretty-print\" 'true'"}

type WalJSocket struct {
	pgConn  *pgconn.PgConn
	pgxConn *pgx.Conn

	// extra copy of db config is required to establish a new db connection
	// which is required to take snapshot data
	dbConfig                   pgconn.Config
	ctx                        context.Context
	cancel                     context.CancelFunc
	clientXLogPos              pglogrepl.LSN
	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	messages                   chan Wal2JsonChanges
	snapshotMessages           chan Wal2JsonChanges
	snapshotName               string
	changeFilter               ChangeFilter
	lsnrestart                 pglogrepl.LSN
	slotName                   string
	schema                     string
	tableSchemas               []schemas.DataTableSchema
	// tableNames                 []string
	separateChanges            bool
	snapshotBatchSize          int
	snapshotMemorySafetyFactor float64
}

// func (s *WalJSocket) ExportedSlot() string {
// 	return fmt.Sprintf("%s_exported", s.slotName)
// }

func NewConnection(config Config) (*WalJSocket, error) {
	var (
		cfg *pgconn.Config
		err error
	)

	sslVerifyFull := ""
	if config.TlsVerify == TlsRequireVerify {
		sslVerifyFull = "&sslmode=verify-full"
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?%s",
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
		sslVerifyFull,
	)

	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		return nil, err
	}

	if cfg, err = pgconn.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database%s",
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
		sslVerifyFull,
	)); err != nil {
		return nil, err
	}

	if config.TlsVerify == TlsRequireVerify {
		cfg.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         config.Host,
		}
	}

	dbConn, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	// var tableNames []string
	// var dataSchemas []schemas.DataTableSchema
	// for _, table := range config.DbTablesSchema {
	// 	tableNames = append(tableNames, strings.Split(table.Table, ".")[1])
	// 	var dts schemas.DataTableSchema
	// 	dts.TableName = table.Table
	// 	var arrowSchemaFields []arrow.Field
	// 	for _, col := range table.Columns {
	// 		arrowSchemaFields = append(arrowSchemaFields, arrow.Field{
	// 			Name:     col.Name,
	// 			Type:     helpers.MapPlainTypeToArrow(col.DatabrewType),
	// 			Nullable: col.Nullable,
	// 			Metadata: arrow.Metadata{},
	// 		})
	// 	}
	// 	dts.Schema = arrow.NewSchema(arrowSchemaFields, nil)
	// 	dataSchemas = append(dataSchemas, dts)
	// }

	dataSchemas := []schemas.DataTableSchema{{
		TableName: "public.cdc_test",
		Schema: arrow.NewSchema([]arrow.Field{{
			Name:     "id",
			Type:     helpers.MapPlainTypeToArrow("Int64"),
			Nullable: false,
		}, {
			Name:     "detail",
			Type:     helpers.MapPlainTypeToArrow(""),
			Nullable: false,
		}}, nil),
	}}
	// dataSchemas := []schemas.DataTableSchema{{
	// 	TableName: "public.users",
	// 	Schema: arrow.NewSchema([]arrow.Field{{
	// 		Name:     "id",
	// 		Type:     helpers.MapPlainTypeToArrow("Int64"),
	// 		Nullable: false,
	// 	}, {
	// 		Name:     "name",
	// 		Type:     helpers.MapPlainTypeToArrow(""),
	// 		Nullable: false,
	// 	}, {
	// 		Name:     "email",
	// 		Type:     helpers.MapPlainTypeToArrow(""),
	// 		Nullable: false,
	// 	}, {
	// 		Name:     "created_at",
	// 		Type:     helpers.MapPlainTypeToArrow("Timestamp"),
	// 		Nullable: false,
	// 	}}, nil),
	// }}

	stream := &WalJSocket{
		pgConn:                     dbConn,
		pgxConn:                    conn,
		dbConfig:                   *cfg,
		messages:                   make(chan Wal2JsonChanges),
		snapshotMessages:           make(chan Wal2JsonChanges, 100),
		slotName:                   config.ReplicationSlotName,
		schema:                     config.Schema,
		tableSchemas:               dataSchemas,
		snapshotMemorySafetyFactor: config.SnapshotMemorySafetyFactor,
		separateChanges:            config.SeparateChanges,
		snapshotBatchSize:          config.BatchSize,
		// tableNames:                 tableNames,
		changeFilter: NewChangeFilter(dataSchemas, config.Schema),
	}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), stream.pgConn)
	if err != nil {
		return nil, fmt.Errorf("failed to identify the system: %s", err)
	}

	logrus.Info("System identification result", "SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "Database:", sysident.DBName)

	var confirmedLSNFromDB string
	// check is replication slot exist to get last restart SLN
	connExecResult := stream.pgConn.Exec(context.TODO(), fmt.Sprintf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'", config.ReplicationSlotName))
	if slotCheckResults, err := connExecResult.ReadAll(); err != nil {
		return nil, fmt.Errorf("failed to read table[pg_replication_slots]: %s", err)
	} else {
		if len(slotCheckResults) == 0 || len(slotCheckResults[0].Rows) == 0 {
			return nil, fmt.Errorf("slot[%s] doesn't exists", config.ReplicationSlotName)
		} else {
			slotCheckRow := slotCheckResults[0].Rows[0]
			confirmedLSNFromDB = string(slotCheckRow[0])
			logrus.Info("Replication slot restart LSN extracted from DB", "LSN", confirmedLSNFromDB)
		}
	}

	lsnrestart, err := pglogrepl.ParseLSN(confirmedLSNFromDB)
	if err != nil {
		return nil, fmt.Errorf("failed to parse LSN: %s", err)
	}

	stream.lsnrestart = lsnrestart
	stream.clientXLogPos = lsnrestart

	stream.standbyMessageTimeout = time.Second * 10
	stream.nextStandbyMessageDeadline = time.Now().Add(stream.standbyMessageTimeout)
	stream.ctx, stream.cancel = context.WithCancel(context.Background())

	if !config.StreamOldData {
		err := stream.startLr()
		if err != nil {
			return nil, err
		}

		go stream.streamMessagesAsync()
	} else {
		// here we create a new replication slot with exported data
		// createSlotResult, err := pglogrepl.CreateReplicationSlot(context.Background(), stream.pgConn, stream.ExportedSlot(), outputPlugin,
		// 	pglogrepl.CreateReplicationSlotOptions{
		// 		Temporary:      false,
		// 		SnapshotAction: "export",
		// 		Mode:           pglogrepl.LogicalReplication,
		// 	})
		// if err != nil {
		// 	logrus.Fatalf("Failed to create replication slot for the database: %s", err.Error())
		// }
		// stream.snapshotName = createSlotResult.SnapshotName

		fmt.Println(stream.snapshotName)
		// New messages will be streamed after the snapshot has been processed.
		go stream.processSnapshot()
	}

	return stream, err
}

func (s *WalJSocket) startLr() error {
	err := pglogrepl.StartReplication(context.Background(), s.pgConn, s.slotName, s.lsnrestart, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return fmt.Errorf("starting replication slot failed: %s", err)
	}
	logrus.Infof("Started logical replication on slot[%s]", s.slotName)

	return nil
}

func (s *WalJSocket) AckLSN(lsn string) {
	var err error
	s.clientXLogPos, err = pglogrepl.ParseLSN(lsn)
	if err != nil {
		logrus.Fatalf("Failed to parse LSN for Acknowledge %s", err.Error())
	}

	err = pglogrepl.SendStandbyStatusUpdate(context.Background(), s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.clientXLogPos,
		WALFlushPosition: s.clientXLogPos,
	})

	if err != nil {
		logrus.Fatalf("SendStandbyStatusUpdate failed: %s", err.Error())
	}
	logrus.Debugf("Sent Standby status message at LSN#%s", s.clientXLogPos.String())
	s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
}

func (s *WalJSocket) streamMessagesAsync() {
	for {
		select {
		case <-s.ctx.Done():
			s.cancel()
			return
		default:
			if time.Now().After(s.nextStandbyMessageDeadline) {
				err := pglogrepl.SendStandbyStatusUpdate(context.Background(), s.pgConn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: s.clientXLogPos,
				})

				if err != nil {
					logrus.Fatalf("SendStandbyStatusUpdate failed: %s", err.Error())
				}
				logrus.Debugf("Sent Standby status message at LSN#%s", s.clientXLogPos.String())
				s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
			}

			ctx, cancel := context.WithDeadline(context.Background(), s.nextStandbyMessageDeadline)
			rawMsg, err := s.pgConn.ReceiveMessage(ctx)
			s.cancel = cancel
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				logrus.Fatalf("Failed to receive messages from PostgreSQL %s", err.Error())
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				logrus.Fatalf("Received broken Postgres WAL. Error: %+v", errMsg)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				logrus.Warnf("Received unexpected message: %T\n", rawMsg)
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					logrus.Fatalf("ParsePrimaryKeepaliveMessage failed: %s", err.Error())
				}

				if pkm.ReplyRequested {
					s.nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					logrus.Fatalf("ParseXLogData failed: %s", err.Error())
				}
				clientXLogPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				s.changeFilter.FilterChange(clientXLogPos.String(), xld.WALData, func(change Wal2JsonChanges) {
					s.messages <- change
				})
			}
		}
	}
}
func (s *WalJSocket) processSnapshot() {
	snapshotter := NewSnapshotter(s.pgxConn, s.snapshotName)

	if err := snapshotter.Prepare(); err != nil {
		logrus.Errorf("failed to prepare database snapshot: %s", err)
		s.cleanUpOnFailure()
		os.Exit(1)
	}
	defer func() {
		snapshotter.ReleaseSnapshot()
		snapshotter.CloseConn()
	}()

	for _, table := range s.tableSchemas {
		logrus.Info("Processing database snapshot", "schema", s.schema, "table", table)

		var offset = 0

		pk, err := s.getPrimaryKeyColumn(table.TableName)
		if err != nil {
			logrus.Fatalf("Failed to resolve pk %s", err.Error())
		}

		logrus.Info("Query snapshot", "batch-size", s.snapshotBatchSize)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, table.Schema)

		colNames := make([]string, 0, len(table.Schema.Fields()))

		for _, col := range table.Schema.Fields() {
			colNames = append(colNames, pgx.Identifier{col.Name}.Sanitize())
		}

		for {
			var snapshotRows pgx.Rows
			logrus.Info("Query snapshot: ", "table", table.TableName, "columns", colNames, "batch-size", s.snapshotBatchSize, "offset", offset)
			if snapshotRows, err = snapshotter.QuerySnapshotData(table.TableName, colNames, pk, s.snapshotBatchSize, offset); err != nil {
				logrus.Errorf("Failed to query snapshot data %s", err.Error())
				s.cleanUpOnFailure()
				os.Exit(1)
			}

			var rowsCount = 0
			for snapshotRows.Next() {
				rowsCount += 1

				values, err := snapshotRows.Values()
				if err != nil {
					panic(err)
				}

				for i, v := range values {
					s := scalar.NewScalar(table.Schema.Field(i).Type)
					if err := s.Set(v); err != nil {
						panic(err)
					}

					scalar.AppendToBuilder(builder.Field(i), s)
				}
				var snapshotChanges = Wal2JsonChanges{
					Lsn: "",
					Changes: []Wal2JsonChange{
						{
							Kind:   "insert",
							Schema: s.schema,
							Table:  strings.Split(table.TableName, ".")[1],
							Row:    builder.NewRecord(),
						},
					},
				}

				s.snapshotMessages <- snapshotChanges
			}

			snapshotRows.Close()

			offset += s.snapshotBatchSize

			if s.snapshotBatchSize != rowsCount {
				break
			}
		}

	}

	// err := s.startLr()
	// if err != nil {
	// 	panic(err)
	// }
	// go s.streamMessagesAsync()
}

func (s *WalJSocket) OnMessage(callback OnMessage) {
	for {
		select {
		case snapshotMessage := <-s.snapshotMessages:
			callback(snapshotMessage)
		case message := <-s.messages:
			callback(message)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *WalJSocket) SnapshotMessageC() chan Wal2JsonChanges {
	return s.snapshotMessages
}

func (s *WalJSocket) LrMessageC() chan Wal2JsonChanges {
	return s.messages
}

// cleanUpOnFailure drops replication slot and publication if database snapshotting was failed for any reason
func (s *WalJSocket) cleanUpOnFailure() {
	s.pgConn.Close(context.TODO())
	s.pgxConn.Close(context.TODO())
}

func (s *WalJSocket) getPrimaryKeyColumn(tableName string) (string, error) {
	q := fmt.Sprintf(`
		SELECT a.attname
		FROM   pg_index i
		JOIN   pg_attribute a ON a.attrelid = i.indrelid
							 AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = '%s'::regclass
		AND    i.indisprimary;
	`, strings.Split(tableName, ".")[1])

	reader := s.pgConn.Exec(context.Background(), q)
	data, err := reader.ReadAll()
	if err != nil {
		return "", err
	}

	pkResultRow := data[0].Rows[0]
	pkColName := string(pkResultRow[0])
	return pkColName, nil
}

func (s *WalJSocket) Stop() error {
	if s.pgConn != nil {
		if s.ctx != nil {
			s.cancel()
		}

		return s.pgConn.Close(context.TODO())
	}

	return nil
}

func doesReplicationSlotExists(conn *pgx.Conn, slotName string) (bool, error) {
	var exists bool
	err := conn.QueryRow(
		context.Background(),
		"SELECT EXISTS(Select 1 from pg_replication_slots where slot_name = $1)",
		slotName,
	).Scan(&exists)

	if err != nil {
		return false, err
	}

	return exists, nil
}
