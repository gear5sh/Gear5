package waljs

import (
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/piyushsingariya/shift/protocol"
)

type WalJSChange struct {
	Stream    protocol.Stream
	Timestamp *time.Time
	LSN       *pglogrepl.LSN
	Kind      string
	Schema    string
	Table     string
	Data      map[string]any
	// Row       arrow.Record
}

type WALMessage struct {
	// NextLSN   pglogrepl.LSN `json:"nextlsn"`
	Timestamp time.Time `json:"timestamp"`
	Change    []struct {
		Kind         string        `json:"kind"`
		Schema       string        `json:"schema"`
		Table        string        `json:"table"`
		Columnnames  []string      `json:"columnnames"`
		Columntypes  []string      `json:"columntypes"`
		Columnvalues []interface{} `json:"columnvalues"`
		Oldkeys      struct {
			Keynames  []string      `json:"keynames"`
			Keytypes  []string      `json:"keytypes"`
			Keyvalues []interface{} `json:"keyvalues"`
		} `json:"oldkeys"`
	} `json:"change"`
}

type OnMessage = func(message WalJSChange) bool
