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
	tx        pgx.Tx
	stream    protocol.Stream
	batchSize int
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
	// if err := s.export(); err != nil {
	// 	return err
	// }

	tx, err := conn.BeginTx(context.TODO(), pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})

	s.tx = tx

	// res, err := s.conn.Exec(context.TODO(), "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
	// if err != nil {
	// 	return fmt.Errorf("failed to begin transcation: %s", err)
	// }
	// fmt.Println(res.String())

	// res, err = s.conn.Exec(context.TODO(), fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s';", s.snapshotName))
	// if err != nil {
	// 	return fmt.Errorf("failed to set snapshot: %s", err)
	// }
	// fmt.Println(res.RowsAffected())

	return err
}

// func (s *Snapshotter) FindAvgRowSize(table string) sql.NullInt64 {
// 	var avgRowSize sql.NullInt64

// 	if rows, err := s.conn.Query(context.TODO(), fmt.Sprintf(`SELECT SUM(pg_column_size('%s.*')) / COUNT(*) FROM %s;`, table, table)); err != nil {
// 		log.Fatal("Can get avg row size", err)
// 	} else {
// 		if rows.Next() {
// 			if err = rows.Scan(&avgRowSize); err != nil {
// 				log.Fatal("Can get avg row size", err)
// 			}
// 		} else {
// 			log.Fatal("Can get avg row size; 0 rows returned")
// 		}
// 	}

// 	return avgRowSize
// }

// func (s *Snapshotter) CalculateBatchSize(safetyFactor float64, availableMemory uint64, estimatedRowSize uint64) int {
// 	// Adjust this factor based on your system's memory constraints.
// 	// This example uses a safety factor of 0.8 to leave some memory headroom.
// 	batchSize := int(float64(availableMemory) * safetyFactor / float64(estimatedRowSize))
// 	if batchSize < 1 {
// 		batchSize = 1
// 	}
// 	return batchSize
// }

// Deprecated: Use QuerySnapshot
func (s *Snapshotter) QuerySnapshotData(table string, columns []string, pk string, limit, offset int) (rows pgx.Rows, err error) {
	joinedColumns := strings.Join(columns, ", ")
	return s.tx.Query(context.TODO(), fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d OFFSET %d;", joinedColumns, table, pk, limit, offset))
}

// func (s *Snapshotter) QuerySnapshot(offset int) (pgx.Rows, error) {
// 	return s.tx.Query(context.TODO(), fmt.Sprintf("SELECT * FROM %s.%s ORDER BY %s LIMIT %d OFFSET %d;",
// 		s.stream.Name(),
// 		s.stream.Namespace(), s.batchSize, offset))
// }

func (s *Snapshotter) ReleaseSnapshot() error {
	return s.tx.Commit(context.Background())
}

func (s *Snapshotter) CloseConn() error {
	if s.tx != nil {
		return s.tx.Conn().Close(context.Background())
	}

	return nil
}
