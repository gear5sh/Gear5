package constants

type SyncMode string

const (
	FullRefresh SyncMode = "full_refresh"
	Incremental SyncMode = "incremental"
	CDC         SyncMode = "cdc"
)
