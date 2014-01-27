package ibis

type Cluster interface {
	GetKeyspace() string
	Query(...*CQL) Query
	Close()
}

type Query interface {
	Exec() error
	Scan(...interface{}) bool
	ScanCAS(...interface{}) bool
	Close() error
	// TODO: query options, like consistency level, etc.
}
