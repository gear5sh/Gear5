package driver

import (
	kakumodels "github.com/piyushsingariya/kaku/models"
	"github.com/piyushsingariya/kaku/types"
)

type HubspotStream interface {
	ScopeIsGranted(grantedScopes []string) bool
	Name() string
	readRecords(channel chan<- kakumodels.Record) error
	Modes() []types.SyncMode
	PrimaryKey() []string
	path() (string, string)
	state() map[string]any
}
