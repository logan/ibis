package datastore

import "flag"
import "fmt"
import "strings"

var (
	flagCluster  = flag.String("cluster", "localhost", "cassandra nodes given as comma-separated host:port pairs")
	flagKeyspace = flag.String("keyspace", "creative_test", "name of throwaway keyspace for testing")
)

// A TestConn extends CassandraConn to manage throwaway keyspaces. This guarantees tests a pristine
// environment before interacting with Cassandra.
type TestConn struct {
	*CassandraConn
}

// NewTestConn connects to Cassandra and establishes an empty keyspace to operate in.
func NewTestConn() (*TestConn, error) {
	config := CassandraConfig{
		Node:        strings.Split(*flagCluster, ","),
		Keyspace:    "system",
		Consistency: "one",
	}
	if err := initKeyspace(config); err != nil {
		return nil, err
	}

	config.Keyspace = *flagKeyspace
	c, err := DialCassandra(config)
	if err != nil {
		return nil, err
	}

	return &TestConn{c}, nil
}

func initKeyspace(config CassandraConfig) error {
	c, err := DialCassandra(config)
	if err != nil {
		return err
	}
	defer c.Close()

	q := c.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", *flagKeyspace))
	if err := q.Exec(); err != nil {
		return err
	}

	q = c.Query(fmt.Sprintf("CREATE KEYSPACE %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}", *flagKeyspace))
	return q.Exec()
}

// Close drops the keyspace and closes the session.
func (tc *TestConn) Close() error {
	q := tc.Query(fmt.Sprintf("DROP KEYSPACE %s", *flagKeyspace))
	if err := q.Exec(); err != nil {
		return err
	}
	tc.Session.Close()
	return nil
}
