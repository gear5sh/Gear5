package constants

type Action string

const (
	TRUNCATE Action = "TRUNCATE"
	CREATE   Action = "CREATE"
	DROP     Action = "DROP"
	ALTER    Action = "ALTER"
)
