package driver

import (
	syndicatemodels "github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/types"
)

type HubspotStream interface {
	ScopeIsGranted(grantedScopes []string) bool
	Name() string
	readRecords(channel chan<- syndicatemodels.Record) error
	Modes() []types.SyncMode
	PrimaryKey() []string
	path() (string, string)
}
