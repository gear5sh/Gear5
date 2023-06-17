package driver

type HubspotStream interface {
	ScopeIsGranted(grantedScopes []string) bool
}
