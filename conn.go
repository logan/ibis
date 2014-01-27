package ibis

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

// cassandraConn is an open connection to a Cassandra cluster associated with a particular keyspace.
type cassandraConn struct {
	*gocql.Session                 // The underlying gocql Session, for querying the cluster.
	Config         CassandraConfig // The settings used to establish the session.
}

// DialCassandra connects to a Cassandra cluster as specified by the given config.
func DialCassandra(config CassandraConfig) (Cluster, error) {
	var session *gocql.Session
	var err error
	if session, err = makeCluster(config).CreateSession(); err != nil {
		return nil, err
	}
	return &cassandraConn{Config: config, Session: session}, nil
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

func (conn *cassandraConn) GetKeyspace() string {
	return conn.Config.Keyspace
}

func (conn *cassandraConn) Query(stmts ...CQL) Query {
	if len(stmts) == 0 {
		return nil
	}
	if len(stmts) > 1 {
		return conn.queryBatch(stmts)
	}
	return conn.query(stmts[0])
}

func (conn *cassandraConn) query(stmt CQL) Query {
	return (*cassQuery)(conn.Session.Query(string(stmt.PreparedCQL), stmt.params...).Iter())
}

func (conn *cassandraConn) queryBatch(stmts []CQL) Query {
	batch := gocql.NewBatch(gocql.LoggedBatch)
	for _, stmt := range stmts {
		batch.Query(string(stmt.PreparedCQL), stmt.params...)
	}
	return &cassBatchQuery{conn.Session.ExecuteBatch(batch)}
}

type cassBatchQuery struct{ error }

func (iter *cassBatchQuery) Close() error                     { return iter.error }
func (iter *cassBatchQuery) Exec() error                      { return iter.Close() }
func (iter *cassBatchQuery) ScanCAS(dest ...interface{}) bool { return false }
func (iter *cassBatchQuery) Scan(dest ...interface{}) bool    { return false }

type cassQuery gocql.Iter

func (iter *cassQuery) Close() error {
	return (*gocql.Iter)(iter).Close()
}

func (iter *cassQuery) Exec() error {
	return iter.Close()
}

func (iter *cassQuery) ScanCAS(dest ...interface{}) bool {
	// As of 2014-01-23, gocql.Iter.Close has no side effect.
	if iter.Close() != nil {
		return false
	}
	var applied bool
	i := (*gocql.Iter)(iter)
	if len(i.Columns()) > 1 {
		dest = append([]interface{}{&applied}, dest...)
		i.Scan(dest...)
	} else {
		i.Scan(&applied)
	}
	return applied
}

func (iter *cassQuery) Scan(dest ...interface{}) bool {
	return (*gocql.Iter)(iter).Scan(dest...)
}
