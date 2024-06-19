package waljs

import (
	"crypto/tls"
	"net/url"
	"time"

	"github.com/gear5sh/gear5/protocol"
	"github.com/gear5sh/gear5/types"
	"github.com/gear5sh/gear5/typeutils"
	"github.com/jackc/pglogrepl"
)

type Config struct {
	Tables                     *types.Set[protocol.Stream]
	FullSyncTables             *types.Set[protocol.Stream] // full sync tables must be a subset of ChangeTables
	Connection                 url.URL
	ReplicationSlotName        string
	InitialWaitTime            time.Duration
	SnapshotMemorySafetyFactor float64
	TLSConfig                  *tls.Config
	State                      *types.Global[*WALState]
}

type WALState struct {
	LSN string `json:"lsn"`
}

func (s *WALState) IsEmpty() bool {
	return s.LSN == ""
}

type ReplicationSlot struct {
	SlotType string        `db:"slot_type"`
	Plugin   string        `db:"plugin"`
	LSN      pglogrepl.LSN `db:"confirmed_flush_lsn"`
}

type WalJSChange struct {
	Stream    protocol.Stream
	Timestamp *typeutils.Time
	LSN       *pglogrepl.LSN
	Kind      string
	Schema    string
	Table     string
	Data      map[string]any
}

type WALMessage struct {
	// NextLSN   pglogrepl.LSN `json:"nextlsn"`
	Timestamp typeutils.Time `json:"timestamp"`
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

type OnMessage = func(message WalJSChange) (bool, error)
