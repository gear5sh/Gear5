package waljs

import (
	"context"
	"fmt"
	"strings"

	"github.com/piyushsingariya/shift/protocol"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
)

type Snapshotter struct {
	tx     pgx.Tx
	stream protocol.Stream
}

func NewSnapshotter(stream protocol.Stream, batchSize int) *Snapshotter {
	return &Snapshotter{
		stream: stream,
	}
}

// func (s *Snapshotter) export() error {
// 	res, err := s.conn.Exec(context.TODO(), "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
// 	if err != nil {
// 		return fmt.Errorf("failed to begin transcation: %s", err)
// 	}
// 	fmt.Println(res.String())

// 	rows, err := s.conn.Query(context.TODO(), "SELECT pg_export_snapshot();")
// 	if err != nil {
// 		return fmt.Errorf("failed to export data: %s", err)
// 	}

// 	fmt.Println(res)

// 	for rows.Next() {
// 		result := ""

// 		err = rows.Scan(&result)
// 		if err != nil {
// 			return err
// 		}

// 		s.snapshotName = result
// 	}

// 	return nil
// }

func (s *Snapshotter) Prepare(conn *pgx.Conn) error {
	tx, err := conn.BeginTx(context.TODO(), pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})

	s.tx = tx

	return err
}

// Deprecated: Use QuerySnapshot
func (s *Snapshotter) QuerySnapshotData(table string, columns []string, pk string, limit, offset int) (rows pgx.Rows, err error) {
	joinedColumns := strings.Join(columns, ", ")
	return s.tx.Query(context.TODO(), fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d OFFSET %d;", joinedColumns, table, pk, limit, offset))
}

func (s *Snapshotter) ReleaseSnapshot() error {
	return s.tx.Commit(context.Background())
}

func (s *Snapshotter) CloseConn() error {
	if s.tx != nil {
		return s.tx.Conn().Close(context.Background())
	}

	return nil
}
