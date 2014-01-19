package datastore

import "testing"

type TestOrm struct {
	*Orm
	*TestConn
	M *TestModel
}

func NewTestOrm(t *testing.T) *TestOrm {
	tc := NewTestConn(t)
	model := &TestModel{}
	schema := ReflectSchemaFrom(model)
	orm := &Orm{CassandraConn: tc.CassandraConn, Model: schema}
	schema.Bind(orm)
	var err error
	if orm.SchemaUpdates, err = DiffLiveSchema(tc.CassandraConn, schema); err != nil {
		t.Fatal(err)
	}
	if err = orm.ApplySchemaUpdates(); err != nil {
		t.Fatal(err)
	}
	return &TestOrm{orm, tc, model}
}

func TestCreateAndLoadByKey(t *testing.T) {
	orm := NewTestOrm(t)
	defer orm.Close()

	row := orm.M.Bags.NewRow().(*BagOfManyTypes)
	row.A = true
	row.C = 1
	row.D = "x"
	cf := (*ColumnFamily)(orm.M.Bags)
	if err := cf.CommitCAS(row); err != nil {
		t.Fatal(err)
	}

	row_out := orm.M.Bags.NewRow().(*BagOfManyTypes)
	if err := cf.LoadByKey(row_out, "x", 1, true); err != nil {
		t.Fatal(err)
	}
	if !rowsEqual(row, row_out) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", *row, row_out)
	}

	if err := cf.CommitCAS(row); err != ErrAlreadyExists {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
	// reconstruct to clear loadedColumns()
	row = orm.M.Bags.NewRow().(*BagOfManyTypes)
	row.A = true
	row.C = 1
	row.D = "x"
	if err := cf.CommitCAS(row); err != ErrAlreadyExists {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}
