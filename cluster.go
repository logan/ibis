package ibis

// Cluster is an interface for a Cassandra session.
type Cluster interface {
	// GetKeyspace returns the name of the current keyspace.
	GetKeyspace() string

	// Query executes CQL and returns an implementation of the Query interface for accessing the
	// result. If multiple statements are given, they are executed together in a batch.
	Query(...CQL) Query

	// Close shuts down the session.
	Close()
}

// Query is an interface for delivering the results of executing CQL on a cluster.
type Query interface {
	// Exec ensures the CQL is executed and returns any error that occurred.
	Exec() error

	// Scan reads a row into the given pointers and returns true on success. The pointers given
	// should correspond to the columns selected by the CQL that was executed. Scan returns false
	// if no more rows are available, or if there is an error. Call Close() on the query to
	// determine which case.
	Scan(...interface{}) bool

	// ScanCAS reads a row into the given pointers and returns true on success. This should only be
	// called on a query for an INSERT INTO ... IF NOT EXISTS statement. The first pointer should be
	// to a bool value, which will be false if the inserted row already exists. Returns false if a
	// row already exists or if an error occurred. Call Close() on the query to determine which
	// case.
	ScanCAS(...interface{}) bool

	// Close finalizes the query and returns any error that occurred. If the query executed
	// successfully and there was no error scanning the results, then nil is returned.
	Close() error

	// TODO: query options, like consistency level, prefetch, etc.
}
