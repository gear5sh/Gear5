package waljs

import (
	"crypto/tls"
	"net/url"

	"github.com/piyushsingariya/shift/protocol"
	"github.com/piyushsingariya/shift/types"
)

type Config struct {
	Tables                     *types.Set[protocol.Stream]
	FullSyncTables             *types.Set[protocol.Stream] // full sync tables must be a subset of ChangeTables
	Connection                 url.URL
	ReplicationSlotName        string
	InitialWaitTime            int
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
