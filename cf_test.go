package ibis

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

	cf := &ColumnFamily{}
	cf.fillFromRowType("test", reflect.TypeOf(table{}))
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

type crudTable ColumnFamily

func (t *crudTable) CF() *ColumnFamily {
	return (*ColumnFamily)(t)
}

func (t *crudTable) ConfigureCF(cf *ColumnFamily) {
	cf.Key("Partition", "Cluster")
	cf.Reflect(crudRow{})
}

type crudModel struct {
	*crudTable
}

func newCrudModel(t *testing.T) *crudModel {
	cluster := NewTestConn(t)
	model := &crudModel{}
	schema := ReflectSchemaFrom(model)
	schema.Cluster = cluster

	var err error
	if schema.SchemaUpdates, err = DiffLiveSchema(cluster, schema); err != nil {
		t.Fatal(err)
	}
	if err = schema.ApplySchemaUpdates(); err != nil {
		t.Fatal(err)
	}
	return model
}

func TestCrud(t *testing.T) {
	model := newCrudModel(t)
	defer model.crudTable.CF().Cluster().Close()

	crud := crudRow{Partition: "P1", Cluster: 0, Value: "P1-0"}
	if err := model.crudTable.CF().CommitCAS(&crud); err != nil {
		t.Fatal(err)
	}
	err := model.crudTable.CF().CommitCAS(&crud)
	if err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	crud = crudRow{}
	if err = model.crudTable.CF().LoadByKey(&crud, "P1", 0); err != nil {
		t.Fatal(err)
	}
	if crud.Partition != "P1" || crud.Cluster != 0 || crud.Value != "P1-0" {
		t.Error("LoadByKey didn't fill in what we expected: %+v", crud)
	}
	if err = model.crudTable.CF().LoadByKey(&crud, "P1", 1); err == nil {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	crud.Cluster = 1
	crud.Value = "P1-1"
	if err := model.crudTable.CF().CommitCAS(&crud); err != nil {
		t.Fatal(err)
	}
	loaded := crudRow{}
	if err = model.crudTable.CF().LoadByKey(&loaded, "P1", 1); err != nil {
		t.Fatal(err)
	}
	if loaded.Value != "P1-1" {
		t.Errorf("expected loaded to have value of P1-1, got: %+v", loaded)
	}

	crud.Value = "P1-1 modified"
	if err := model.crudTable.CF().Commit(&crud); err != nil {
		t.Fatal(err)
	}
	loaded = crudRow{}
	if err = model.crudTable.CF().LoadByKey(&loaded, "P1", 1); err != nil {
		t.Fatal(err)
	}
	if loaded.Value != "P1-1 modified" {
		t.Errorf("expected loaded to have value of P1-1 modified, got: %+v", loaded)
	}

	var b bool
	if b, err = model.crudTable.CF().Exists("P2", 0); err != nil {
		t.Fatal(err)
	}
	if b {
		t.Fatal("Exists should have returned false")
	}
	if b, err = model.crudTable.CF().Exists("P1", 0); err != nil {
		t.Fatal(err)
	}
	if !b {
		t.Fatal("Exists should have returned true")
	}
}
