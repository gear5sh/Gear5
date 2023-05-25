package constants

type MessageType string

const (
	LogType              MessageType = "LOG"
	ConnectionStatusType MessageType = "CONNECTION_STATUS"
	StateType            MessageType = "STATE"
	RecordType           MessageType = "RECORD"
	CatalogType          MessageType = "CATALOG"
	SpecType             MessageType = "SPEC"
)
