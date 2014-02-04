package ibis

import "flag"
import "strconv"
import "strings"
import "testing"

var (
	flagCluster  = flag.String("cluster", "", "cassandra nodes given as comma-separated host:port pairs")
	flagKeyspace = flag.String("keyspace", "creative_test", "name of throwaway keyspace for testing")
)

type FakeSeqIDGenerator uint64

func (g *FakeSeqIDGenerator) Set(next uint64) *FakeSeqIDGenerator {
	*g = FakeSeqIDGenerator(next - 1)
	return g
}

func (g *FakeSeqIDGenerator) NewSeqID() (SeqID, error) {
	*g++
	return SeqID(strconv.FormatUint(uint64(*g), 36)), nil
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

// NewTestConn connects to the cluster given by the -cluster flag and establishes an empty keyspace
// to operate in. If -cluster is not given or empty, FakeCassandra is used, which may not be
// representative of real Cassandra behavior but is faster and more suitable for unit testing.
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

func ReflectTestSchema(t *testing.T, model interface{}) *Schema {
	schema := ReflectSchema(model)
	schema.Cluster = NewTestConn(t)

	var err error
	if schema.SchemaUpdates, err = DiffLiveSchema(schema.Cluster, schema); err != nil {
		t.Fatal(err)
	}
	if err = schema.ApplySchemaUpdates(); err != nil {
		t.Fatal(err)
	}

	return schema
}
