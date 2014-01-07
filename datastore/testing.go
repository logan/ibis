package datastore

import "flag"
import "fmt"
import "strings"

var (
	flagCluster  = flag.String("cluster", "localhost", "cassandra nodes given as comma-separated host:port pairs")
	flagKeyspace = flag.String("keyspace", "creative_test", "name of throwaway keyspace for testing")
)

type TestConn struct {
	*CassandraConn
}

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
	defer c.Session.Close()

	q := c.Session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", *flagKeyspace))
	if err := q.Exec(); err != nil {
		return err
	}

	q = c.Session.Query(fmt.Sprintf("CREATE KEYSPACE %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}", *flagKeyspace))
	return q.Exec()
}

func (tc *TestConn) Close() error {
	s := tc.Session
	q := s.Query(fmt.Sprintf("DROP KEYSPACE %s", *flagKeyspace))
	return q.Exec()
}
