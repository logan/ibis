package datastore

import "testing"
import "time"

type TestOrm struct {
	*Orm
	*TestConn
	M *testModel
}

func NewTestOrm(t *testing.T) *TestOrm {
	tc := NewTestConn(t)
	model := &testModel{}
	schema := ReflectSchemaFrom(model)
	orm := &Orm{CassandraConn: tc.CassandraConn, Model: schema}
	schema.Bind(orm)
	var err error
	if orm.SchemaUpdates, err = DiffLiveSchema(tc.CassandraConn, schema); err != nil {
		t.Fatal(err)
	}
	t.Logf("schema:\n%s", orm.SchemaUpdates)
	if err = orm.ApplySchemaUpdates(); err != nil {
		t.Fatal(err)
	}
	return &TestOrm{orm, tc, model}
}

func TestCreateAndLoadByKeyAndExists(t *testing.T) {
	orm := NewTestOrm(t)
	//defer orm.Close()

	cf := (*ColumnFamily)(orm.M.Bags)
	b, err := cf.Exists("x", 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if b {
		t.Fatal("Exists returned true on empty keyspace")
	}

	row := orm.M.Bags.NewRow().(*bagOfManyTypes)
	row.A = true
	row.C = 1
	row.D = "x"
	if err := cf.CommitCAS(row); err != nil {
		t.Fatal(err)
	}

	b, err = cf.Exists("x", 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if !b {
		t.Fatal("Exists should have returned true")
	}

	row_out := orm.M.Bags.NewRow().(*bagOfManyTypes)
	if err := cf.LoadByKey(row_out, "x", 1, true); err != nil {
		t.Fatal(err)
	}
	if !rowsEqual(row, row_out) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", row, row_out)
	}

	if err := cf.CommitCAS(row); err != ErrAlreadyExists {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
	// reconstruct to clear loadedColumns()
	row = orm.M.Bags.NewRow().(*bagOfManyTypes)
	row.A = true
	row.C = 1
	row.D = "x"
	if err := cf.CommitCAS(row); err != ErrAlreadyExists {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestCommit(t *testing.T) {
	orm := NewTestOrm(t)
	defer orm.Close()

	now := time.Now()
	cf := (*ColumnFamily)(orm.M.Bags)
	row := orm.M.Bags.NewRow().(*bagOfManyTypes)
	row.A = true
	row.C = 1
	row.D = "x"
	if err := cf.CommitCAS(row); err != nil {
		t.Fatal(err)
	}

	row.E = now
	if err := cf.Commit(row); err != nil {
		t.Fatal(err)
	}

	out := orm.M.Bags.NewRow().(*bagOfManyTypes)
	if err := cf.LoadByKey(out, "x", 1, true); err != nil {
		t.Fatal(err)
	}
	if !rowsEqual(row, out) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", *row, out)
	}
}
