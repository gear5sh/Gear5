package driver

import (
	"github.com/piyushsingariya/shift/types"
)

type HubspotStream interface {
	ScopeIsGranted(grantedScopes []string) bool
	Name() string
	readRecords(channel chan<- types.Record) error
	Modes() []types.SyncMode
	PrimaryKey() []string
	path() (string, string)
	state() map[string]any
	setup(mode types.SyncMode, state map[string]any)
	cursorField() string
}
