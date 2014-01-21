package datastore

import "bytes"
import "flag"
import "fmt"
import "reflect"
import "strings"
import "testing"
import "time"

var (
	flagCluster  = flag.String("cluster", "localhost", "cassandra nodes given as comma-separated host:port pairs")
	flagKeyspace = flag.String("keyspace", "creative_test", "name of throwaway keyspace for testing")
)

type BagOfManyTypes struct {
	ReflectedRow
	A bool
	B float64
	C int64
	D string
	E time.Time
	F []byte
}

type BagOfManyTypesTable ColumnFamily

func (t *BagOfManyTypesTable) NewRow() Row {
	row := &BagOfManyTypes{}
	row.CF = (*ColumnFamily)(t)
	return row.Reflect(row)
}

func (t *BagOfManyTypesTable) ConfigureCF(options *CFOptions) {
	options.Key("D", "C", "A")
}

type TestModel struct {
	Bags *BagOfManyTypesTable
}

// A TestConn extends CassandraConn to manage throwaway keyspaces. This guarantees tests a pristine
// environment before interacting with Cassandra.
type TestConn struct {
	*CassandraConn
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

func rowsEqual(row1, row2 Row) bool {
	type1 := reflect.TypeOf(row1)
	if type1 != reflect.TypeOf(row2) {
		return false
	}
	p1 := reflect.ValueOf(row1).Elem().FieldByName("ReflectedRow").Interface().(ReflectedRow)
	p2 := reflect.ValueOf(row2).Elem().FieldByName("ReflectedRow").Interface().(ReflectedRow)
	if len(*p1.loaded) != len(*p2.loaded) {
		return false
	}
	for k, v1 := range *p1.loaded {
		v2, ok := (*p2.loaded)[k]
		if !ok || !bytes.Equal(v1.Bytes, v2.Bytes) {
			return false
		}
	}
	rv1 := make(MarshalledMap)
	if err := row1.Marshal(&rv1); err != nil {
		return false
	}
	rv2 := make(MarshalledMap)
	if err := row2.Marshal(&rv2); err != nil {
		return false
	}
	if !reflect.DeepEqual(rv1, rv2) {
		return false
	}
	return true
}
