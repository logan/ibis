package datastore

import "strings"

import "tux21b.org/v1/gocql"

// CassandraConfig specifies a Cassandra cluster and keyspace to connect to.
type CassandraConfig struct {
	Keyspace string   // Required. The keyspace to use throughout the connection.
	Node     []string // Required. The list of nodes in the cluster, given as <host>:<port> strings.

	// Optional. The default consistency level for the connection. Valid values are one of:
	//
	//   one, two, three, any, all, quorum, localquorum, eachquorum, serial, or localserial.
	//
	// If no value or an invalid value is given, then "quorum" will be used. Matching is case
	// insensitive.
	Consistency string
}

// CassandraConn is an open connection to a Cassandra cluster, associated with a particular keyspace
// and Model definition.
type CassandraConn struct {
	*gocql.Session                 // The underlying gocql Session, for querying the cluster.
	Config         CassandraConfig // The settings used to establish the session.
	Model          *Schema         // The tables (column families) to use in this keyspace.
	SchemaUpdates  *SchemaDiff     // The differences found between the existing column families and the given Model.
}

// DialCassandra connects to a Cassandra cluster as specified by the given config.
func DialCassandra(config CassandraConfig) (*CassandraConn, error) {
	var session *gocql.Session
	var err error
	if session, err = makeCluster(config).CreateSession(); err != nil {
		return nil, err
	}
	return &CassandraConn{Config: config, Session: session}, nil
}

func makeCluster(config CassandraConfig) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(config.Node...)
	cluster.Keyspace = config.Keyspace
	cluster.Consistency = parseConsistency(config.Consistency)
	return cluster
}

func parseConsistency(value string) (consistency gocql.Consistency) {
	switch strings.ToLower(value) {
	default:
	case "quorum":
		consistency = gocql.Quorum
	case "any":
		consistency = gocql.Any
	case "one":
		consistency = gocql.One
	case "two":
		consistency = gocql.Two
	case "three":
		consistency = gocql.Three
	case "all":
		consistency = gocql.All
	case "localquorum":
		consistency = gocql.LocalQuorum
	case "eachquorum":
		consistency = gocql.EachQuorum
	case "serial":
		consistency = gocql.Serial
	case "localserial":
		consistency = gocql.LocalSerial
	}
	return
}

// SetModel associates a Model definition with the connection and queries the cluster to determine
// how it differs from the existing column families. This fills out the SchemaUpdates field, which
// can be used to apply CQL commands to bring the keyspace up to date with the Model.
func (c *CassandraConn) SetModel(model *Schema) error {
	var err error
	c.Model = model
	c.SchemaUpdates, err = DiffLiveSchema(c)
	return err
}

// ApplySchemaUpdates applies any required modifications to the live schema to match the model
// defined in the CassandraConn, as determined when the CassandraConn was created.
func (c *CassandraConn) ApplySchemaUpdates() error {
	return c.SchemaUpdates.Apply(c.Session)
}
