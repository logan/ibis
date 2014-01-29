package ibis

import "errors"
import "fmt"
import "reflect"
import "testing"
import "time"

func TestFillFromRowTypeAndKeyAndCreateStatement(t *testing.T) {
	type table struct {
		Str    string
		Int    int64
		Bool   bool
		Double float64
		Time   time.Time
		Blob   []byte
		SeqID  SeqID
	}

	cf := &ColumnFamily{Name: "test"}
	cf.fillFromRowType(reflect.TypeOf(table{}))
	cf.Key("Str")

	expect := func(expected string) (string, bool) {
		received := cf.CreateStatement().String()
		if expected != received {
			return fmt.Sprintf("\nexpected: %s\nreceived: %s", expected, received), false
		}
		return "", true
	}

	if msg, ok := expect("CREATE TABLE test (Str varchar, Int bigint, Bool boolean," +
		" Double double, Time timestamp, Blob blob, SeqID varchar," +
		" PRIMARY KEY (Str))"); !ok {
		t.Error(msg)
	}

	cf.Key("Double", "Time", "Blob")
	cf.typeID = 8

	if msg, ok := expect("CREATE TABLE test (Double double, Time timestamp, Blob blob," +
		" Str varchar, Int bigint, Bool boolean, SeqID varchar," +
		" PRIMARY KEY (Double, Time, Blob)) WITH comment='8'"); !ok {
		t.Error(msg)
	}
}

type crudRow struct {
	Partition string
	Cluster   int64
	Value     string
}

type crudTable struct {
	*ColumnFamily
}

func (t *crudTable) CF() *ColumnFamily {
	t.ColumnFamily = ReflectColumnFamily(crudRow{})
	return t.ColumnFamily.Key("Partition", "Cluster")
}

func (t *crudTable) crud() *crudTable { return t }

type crudIndexRow struct {
	Partition string
	// TODO: test a counter here
}

type crudIndexTable struct {
	*ColumnFamily
}

func (t *crudIndexTable) CF() *ColumnFamily {
	t.ColumnFamily = ReflectColumnFamily(crudIndexRow{})
	return t.ColumnFamily.Key("Partition")
}

type crudModel struct {
	*crudTable
	*crudIndexTable
	*Schema
}

func (m *crudModel) Close() {
	m.crudTable.ColumnFamily.Cluster.Close()
}

func newCrudModel(t *testing.T) *crudModel {
	cluster := NewTestConn(t)
	model := &crudModel{}
	model.Schema = ReflectSchema(model)
	model.Schema.SetCluster(cluster)

	var err error
	if model.Schema.SchemaUpdates, err = DiffLiveSchema(cluster, model.Schema); err != nil {
		t.Fatal(err)
	}
	if err = model.Schema.ApplySchemaUpdates(); err != nil {
		t.Fatal(err)
	}
	return model
}

func TestCrud(t *testing.T) {
	model := newCrudModel(t)
	defer model.Close()

	crud := crudRow{Partition: "P1", Cluster: 0, Value: "P1-0"}
	if err := model.crudTable.CommitCAS(&crud); err != nil {
		t.Fatal(err)
	}
	err := model.crudTable.CommitCAS(&crud)
	if err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	crud = crudRow{}
	if err = model.crudTable.LoadByKey(&crud, "P1", 0); err != nil {
		t.Fatal(err)
	}
	if crud.Partition != "P1" || crud.Cluster != 0 || crud.Value != "P1-0" {
		t.Error("LoadByKey didn't fill in what we expected: %+v", crud)
	}
	if err = model.crudTable.LoadByKey(&crud, "P1", 1); err == nil {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	crud.Cluster = 1
	crud.Value = "P1-1"
	if err := model.crudTable.CommitCAS(&crud); err != nil {
		t.Fatal(err)
	}
	loaded := crudRow{}
	if err = model.crudTable.LoadByKey(&loaded, "P1", 1); err != nil {
		t.Fatal(err)
	}
	if loaded.Value != "P1-1" {
		t.Errorf("expected loaded to have value of P1-1, got: %+v", loaded)
	}

	crud.Value = "P1-1 modified"
	if err := model.crudTable.Commit(&crud); err != nil {
		t.Fatal(err)
	}
	loaded = crudRow{}
	if err = model.crudTable.LoadByKey(&loaded, "P1", 1); err != nil {
		t.Fatal(err)
	}
	if loaded.Value != "P1-1 modified" {
		t.Errorf("expected loaded to have value of P1-1 modified, got: %+v", loaded)
	}

	var b bool
	if b, err = model.crudTable.Exists("P2", 0); err != nil {
		t.Fatal(err)
	}
	if b {
		t.Fatal("Exists should have returned false")
	}
	if b, err = model.crudTable.Exists("P1", 0); err != nil {
		t.Fatal(err)
	}
	if !b {
		t.Fatal("Exists should have returned true")
	}
}

func TestProvisioning(t *testing.T) {
	model := newCrudModel(t)
	defer model.Close()

	type crudProvider interface {
		crud() *crudTable
	}
	model.crudTable.Provide(crudProvider(model.crudTable))

	var p crudProvider
	if !model.Schema.GetProvider(&p) {
		t.Fatal("GetProvider returned false")
	}
	if p.crud() != model.crudTable {
		t.Fatalf("crudProvider returned something unexpected: %v vs. %v", p.crud(), model.crudTable)
	}
}

func TestPrecommitHooks(t *testing.T) {
	model := newCrudModel(t)
	defer model.Close()

	partErr := errors.New("partErr")
	hook := func(row interface{}) ([]CQL, error) {
		crud := row.(*crudRow)
		if crud.Partition == "" {
			return nil, partErr
		}
		entry := &crudIndexRow{Partition: crud.Partition}
		cql, err := model.crudIndexTable.MakeCommit(entry)
		if err != nil {
			return nil, err
		}
		return []CQL{cql}, nil
	}
	model.crudTable.Precommit(hook)

	crud := crudRow{Partition: "P1", Cluster: 0, Value: "P1-0"}
	if err := model.crudTable.CommitCAS(&crud); err != nil {
		t.Fatal(err)
	}

	entry := &crudIndexRow{}
	if err := model.crudIndexTable.LoadByKey(entry, "P1"); err != nil {
		t.Fatal(err)
	}
	if entry.Partition != "P1" {
		t.Errorf("expected entry with partition P1, got: %+v", entry.Partition)
	}

	crud = crudRow{}
	if err := model.crudTable.Commit(&crud); err != partErr {
		t.Error("expected partErr, got", err)
	}
}
