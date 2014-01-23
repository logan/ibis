package datastore

import "bytes"
import "flag"
import "fmt"
import "reflect"
import "strconv"
import "strings"
import "testing"

var (
	flagCluster  = flag.String("cluster", "localhost", "cassandra nodes given as comma-separated host:port pairs")
	flagKeyspace = flag.String("keyspace", "creative_test", "name of throwaway keyspace for testing")
)

type testSeqIDGenerator uint64

func (g *testSeqIDGenerator) New() (SeqID, error) {
	*g++
	return SeqID(strconv.FormatUint(uint64(*g), 36)), nil
}

func (g *testSeqIDGenerator) CurrentInterval() string {
	return interval(SeqID(strconv.FormatUint(uint64(*g), 36)))
}

func rowsEqual(row1, row2 Row) bool {
	type1 := reflect.TypeOf(row1)
	if type1 != reflect.TypeOf(row2) {
		return false
	}
	p1 := reflect.ValueOf(row1).Elem().FieldByName("ReflectedRow").Interface().(ReflectedRow)
	p2 := reflect.ValueOf(row2).Elem().FieldByName("ReflectedRow").Interface().(ReflectedRow)
	if len(p1.loaded) != len(p2.loaded) {
		return false
	}
	for k, v1 := range p1.loaded {
		v2, ok := p2.loaded[k]
		if !ok || !bytes.Equal(v1.Bytes, v2.Bytes) {
			return false
		}
	}
	rv1 := make(MarshalledMap)
	if err := row1.Marshal(rv1); err != nil {
		return false
	}
	rv2 := make(MarshalledMap)
	if err := row2.Marshal(rv2); err != nil {
		return false
	}
	if !reflect.DeepEqual(rv1, rv2) {
		return false
	}
	return true
}

// A TestConn extends Cluster to manage throwaway keyspaces. This guarantees tests a pristine
// environment before interacting with Cassandra.
type TestConn struct {
	Cluster
}

// NewTestConn connects to Cassandra and establishes an empty keyspace to operate in.
func NewTestConn(t *testing.T) *TestConn {
	config := CassandraConfig{
		Node:        strings.Split(*flagCluster, ","),
		Keyspace:    "system",
		Consistency: "one",
	}
	if err := initKeyspace(config); err != nil {
		t.Fatal(err)
	}

	config.Keyspace = *flagKeyspace
	c, err := DialCassandra(config)
	if err != nil {
		t.Fatal(err)
	}

	return &TestConn{c}
}

func initKeyspace(config CassandraConfig) error {
	c, err := DialCassandra(config)
	if err != nil {
		return err
	}
	defer c.Close()

	cql := NewCQL(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", *flagKeyspace))
	qiter := c.Query(cql)
	if err := qiter.Exec(); err != nil {
		return err
	}

	cql = NewCQL(fmt.Sprintf("CREATE KEYSPACE %s WITH REPLICATION"+
		" = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		*flagKeyspace))
	return c.Query(cql).Exec()
}

// Close drops the keyspace and closes the session.
func (tc *TestConn) Close() error {
	defer tc.Cluster.Close()
	cql := NewCQL(fmt.Sprintf("DROP KEYSPACE %s", *flagKeyspace))
	return tc.Query(cql).Exec()
}
