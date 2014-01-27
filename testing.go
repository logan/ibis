package ibis

import "bytes"
import "flag"
import "reflect"
import "strconv"
import "strings"
import "testing"

var (
	flagCluster  = flag.String("cluster", "", "cassandra nodes given as comma-separated host:port pairs")
	flagKeyspace = flag.String("keyspace", "creative_test", "name of throwaway keyspace for testing")
)

type testSeqIDGenerator uint64

func (g *testSeqIDGenerator) NewSeqID() (SeqID, error) {
	*g++
	return SeqID(strconv.FormatUint(uint64(*g), 36)), nil
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

func connect(config CassandraConfig) (Cluster, error) {
	if config.Node[0] == "" {
		return FakeCassandra(), nil
	}
	return DialCassandra(config)
}

type testConn struct {
	Cluster
}

// NewTestConn connects to Cassandra and establishes an empty keyspace to operate in.
func NewTestConn(t *testing.T) Cluster {
	config := CassandraConfig{
		Node:        strings.Split(*flagCluster, ","),
		Keyspace:    "system",
		Consistency: "one",
	}
	if err := initKeyspace(config); err != nil {
		t.Fatal(err)
	}

	config.Keyspace = *flagKeyspace
	c, err := connect(config)
	if err != nil {
		t.Fatal(err)
	}

	return &testConn{c}
}

func initKeyspace(config CassandraConfig) error {
	c, err := connect(config)
	if err != nil {
		return err
	}
	defer c.Close()

	var b CQLBuilder
	cql := b.Append("DROP KEYSPACE IF EXISTS ").Append(*flagKeyspace).CQL()
	cql.Cluster(c)
	qiter := cql.Query()
	if err := qiter.Exec(); err != nil {
		return err
	}

	b.Clear()
	b.Append("CREATE KEYSPACE ").Append(*flagKeyspace)
	b.Append(" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	cql = b.CQL()
	cql.Cluster(c)
	return cql.Query().Exec()
}

func (tc *testConn) Close() {
	defer tc.Cluster.Close()
	var b CQLBuilder
	cql := b.Append("DROP KEYSPACE ").Append(*flagKeyspace).CQL()
	cql.Cluster(tc)
	cql.Query().Exec()
}
