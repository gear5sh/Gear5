package waljs

import (
	"crypto/tls"
	"net/url"

	"github.com/piyushsingariya/shift/protocol"
	"github.com/piyushsingariya/shift/types"
)

type Config struct {
	ChangeTables               *types.Set[protocol.Stream]
	FullSyncTables             *types.Set[protocol.Stream] // full sync tables must be a subset of ChangeTables
	Connection                 url.URL
	ReplicationSlotName        string
	SnapshotMemorySafetyFactor float64
	TLSConfig                  *tls.Config
	State                      *WALState
}

type WALState struct {
	LSN string `json:"lsn"`
}
