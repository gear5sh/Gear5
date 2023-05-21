package constants

type SyncMode int

const (
	FullRefresh SyncMode = iota
	Incremental
)
