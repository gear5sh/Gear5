package driver

import (
	"github.com/piyushsingariya/syndicate/protocol"
)

type HubspotStream interface {
	protocol.Stream
	ScopeIsGranted(grantedScopes []string) bool
}
