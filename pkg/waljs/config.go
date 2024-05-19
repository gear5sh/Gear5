package waljs

type TlsVerify string

const TlsNoVerify TlsVerify = "none"
const TlsRequireVerify TlsVerify = "require"

type DbSchemaColumn struct {
	Name                string `yaml:"name"`
	DatabrewType        string `yaml:"databrewType"`
	NativeConnectorType string `yaml:"nativeConnectorType"`
	Pk                  bool   `yaml:"pk"`
	Nullable            bool   `yaml:"nullable"`
}

type DbTablesSchema struct {
	Table   string           `yaml:"table"`
	Columns []DbSchemaColumn `yaml:"columns"`
}

type Config struct {
	Host                       string    `yaml:"db_host"`
	Password                   string    `yaml:"db_password"`
	User                       string    `yaml:"db_user"`
	Port                       int       `yaml:"db_port"`
	Database                   string    `yaml:"db_name"`
	Schema                     string    `yaml:"db_schema"`
	ReplicationSlotName        string    `yaml:"replication_slot_name"`
	TlsVerify                  TlsVerify `yaml:"tls_verify"`
	StreamOldData              bool      `yaml:"stream_old_data"`
	SeparateChanges            bool      `yaml:"separate_changes"`
	SnapshotMemorySafetyFactor float64   `yaml:"snapshot_memory_safety_factor"`
	BatchSize                  int       `yaml:"batch_size"`
}
