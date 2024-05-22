package waljs

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/piyushsingariya/shift/logger"
	"github.com/piyushsingariya/shift/pkg/jdbc"
)

var pluginArguments = []string{"\"pretty-print\" 'false'"}

type Socket struct {
	*Config
	pgConn  *pgconn.PgConn
	pgxConn *pgx.Conn

	ctx                        context.Context // Context to use Inital Wait Time
	cancel                     context.CancelFunc
	clientXLogPos              pglogrepl.LSN
	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	messages                   chan Wal2JsonChanges
	err                        chan error
	changeFilter               ChangeFilter
	lsnrestart                 pglogrepl.LSN
}

func NewConnection(config *Config) (*Socket, error) {
	if !config.FullSyncTables.SubsetOf(config.Tables) {
		return nil, fmt.Errorf("mismatch: full sync tables are not subset of all tables")
	}

	conn, err := pgx.Connect(context.Background(), config.Connection.String())
	if err != nil {
		return nil, err
	}

	query := config.Connection.Query()
	query.Add("replication", "database")
	config.Connection.RawQuery = query.Encode()

	cfg, err := pgconn.ParseConfig(config.Connection.String())
	if err != nil {
		return nil, err
	}

	if config.TLSConfig != nil {
		cfg.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	dbConn, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	connection := &Socket{
		Config:                config,
		standbyMessageTimeout: time.Second,
		pgConn:                dbConn,
		pgxConn:               conn,
		messages:              make(chan Wal2JsonChanges),
		err:                   make(chan error),
		changeFilter:          NewChangeFilter(config.Tables.Array()...),
	}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), connection.pgConn)
	if err != nil {
		return nil, fmt.Errorf("failed to identify the system: %s", err)
	}

	logger.Info("System identification result", "SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "Database:", sysident.DBName)

	var confirmedLSNFromDB string
	// check is replication slot exist to get last restart SLN
	connExecResult := connection.pgConn.Exec(context.TODO(), fmt.Sprintf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'", config.ReplicationSlotName))
	if slotCheckResults, err := connExecResult.ReadAll(); err != nil {
		return nil, fmt.Errorf("failed to read table[pg_replication_slots]: %s", err)
	} else {
		if len(slotCheckResults) == 0 || len(slotCheckResults[0].Rows) == 0 {
			return nil, fmt.Errorf("slot[%s] doesn't exists", config.ReplicationSlotName)
		} else {
			slotCheckRow := slotCheckResults[0].Rows[0]
			confirmedLSNFromDB = string(slotCheckRow[0])
			logger.Info("Replication slot restart LSN extracted from DB", "LSN", confirmedLSNFromDB)
		}
	}

	lsnrestart, err := pglogrepl.ParseLSN(confirmedLSNFromDB)
	if err != nil {
		return nil, fmt.Errorf("failed to parse LSN: %s", err)
	}

	connection.lsnrestart = lsnrestart
	connection.clientXLogPos = lsnrestart

	// Setup initial wait timeout to be the next message deadline to wait for a change log
	connection.nextStandbyMessageDeadline = time.Now().Add(time.Second*time.Duration(config.InitialWaitTime) + time.Second)
	connection.ctx, connection.cancel = context.WithTimeout(context.TODO(), time.Second*time.Duration(config.InitialWaitTime))

	go connection.start()
	return connection, err
}

func (s *Socket) startLr() error {
	err := pglogrepl.StartReplication(context.Background(), s.pgConn, s.ReplicationSlotName, s.lsnrestart, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return fmt.Errorf("starting replication slot failed: %s", err)
	}
	logger.Infof("Started logical replication on slot[%s]", s.ReplicationSlotName)

	return nil
}

// Confirm that Logs has been recorded
func (s *Socket) AcknowledgeLSN(lsn pglogrepl.LSN) error {
	err := pglogrepl.SendStandbyStatusUpdate(context.Background(), s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
	})
	if err != nil {
		return fmt.Errorf("SendStandbyStatusUpdate failed: %s", err)
	}

	// Update local pointer and state
	s.clientXLogPos = lsn
	s.Config.State.LSN = lsn.String()

	logger.Debugf("Sent Standby status message at LSN#%s", s.clientXLogPos.String())
	return nil
}

func (s *Socket) increaseDeadline() {
	s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
}

func (s *Socket) deadlineCrossed() bool {
	return time.Now().After(s.nextStandbyMessageDeadline)
}

func (s *Socket) streamMessagesAsync() {
	var cachedLSN *pglogrepl.LSN

	for {
		select {
		case <-s.ctx.Done():
			//  Initial Wait timeout is triggered
			s.cancel()
			s.err <- nil
			return
		default:
			func() {
				if s.deadlineCrossed() {
					if cachedLSN != nil {
						err := s.AcknowledgeLSN(*cachedLSN)
						if err != nil {
							s.err <- err
							return
						}
					}

					s.err <- nil
					return
				}

				ctx, cancel := context.WithDeadline(context.Background(), s.nextStandbyMessageDeadline)
				defer cancel()

				rawMsg, err := s.pgConn.ReceiveMessage(ctx)
				if err != nil {
					if pgconn.Timeout(err) {
						return
					}
					s.err <- fmt.Errorf("failed to receive messages from PostgreSQL %s", err)
					return
				}

				if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
					s.err <- fmt.Errorf("received broken Postgres WAL. Error: %+v", errMsg)
					return
				}

				msg, ok := rawMsg.(*pgproto3.CopyData)
				if !ok {
					logger.Warnf("Received unexpected message: %T\n", rawMsg)
					return
				}

				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						s.err <- fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %s", err)
						return
					}

					if pkm.ReplyRequested {
						s.nextStandbyMessageDeadline = time.Time{}
					}

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						s.err <- fmt.Errorf("ParseXLogData failed: %s", err)
						return
					}

					// Cache LSN here to be used during acknowledgement
					clientXLogPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
					cachedLSN = &clientXLogPos
					s.changeFilter.FilterChange(clientXLogPos.String(), xld.WALData, func(change Wal2JsonChanges) {
						s.messages <- change
					})
				}

				s.increaseDeadline()
			}()
		}
	}
}

func (s *Socket) start() {
	for _, stream := range s.Config.FullSyncTables.Array() {
		err := func() error {
			snapshotter := NewSnapshotter(stream, int(stream.BatchSize()))
			if err := snapshotter.Prepare(s.pgxConn); err != nil {
				return fmt.Errorf("failed to prepare database snapshot: %s", err)
			}

			defer func() {
				snapshotter.ReleaseSnapshot()
				snapshotter.CloseConn()
			}()

			logger.Infof("Processing database snapshot: %s", stream.ID())
			schema := stream.Schema().ToArrow()
			logger.Info("Query snapshot", "batch-size", stream.BatchSize())
			builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			baseQuery := fmt.Sprintf("SELECT * FROM %s.%s ORDER BY %s ", stream.Name(),
				stream.Namespace(), strings.Join(stream.GetStream().SourceDefinedPrimaryKey.Array(), ", "))

			setter := jdbc.WithContextOffsetter(context.TODO(), baseQuery, int(stream.BatchSize()), snapshotter.tx.Query)

			return setter.Capture(func(rows pgx.Rows) error {
				values, err := rows.Values()
				if err != nil {
					return err
				}

				for i, v := range values {
					s := scalar.NewScalar(schema.Field(i).Type)
					if err := s.Set(v); err != nil {
						return err
					}

					scalar.AppendToBuilder(builder.Field(i), s)
				}
				var snapshotChanges = Wal2JsonChanges{
					Lsn: "",
					Changes: []Wal2JsonChange{
						{
							Kind:   "insert",
							Schema: stream.Namespace(),
							Table:  stream.Name(),
							Row:    builder.NewRecord(),
						},
					},
				}

				s.messages <- snapshotChanges

				return nil
			})
		}()
		if err != nil {
			s.err <- err
			return
		}
	}

	err := s.startLr()
	if err != nil {
		s.err <- err
		return
	}

	go s.streamMessagesAsync()
}

func (s *Socket) OnMessage(callback OnMessage) error {
	for {
		select {
		case err := <-s.err:
			defer s.cleanUpOnFailure()
			return err
		case message := <-s.messages:
			callback(message)
		case <-s.ctx.Done():
			return nil
		}
	}
}

// cleanUpOnFailure drops replication slot and publication if database snapshotting was failed for any reason
func (s *Socket) cleanUpOnFailure() {
	s.pgConn.Close(context.TODO())
	s.pgxConn.Close(context.TODO())
}

func (s *Socket) Stop() error {
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
