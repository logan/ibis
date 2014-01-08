package datastore

import "testing"

type TestOrm struct {
	*Orm
	*TestConn
}

func NewTestOrm(t *testing.T) *TestOrm {
	tc := NewTestConn(t)
	orm := &Orm{CassandraConn: tc.CassandraConn, Model: testModel}
	var err error
	if orm.SchemaUpdates, err = DiffLiveSchema(tc.CassandraConn, testModel); err != nil {
		t.Fatal(err)
	}
	if err = orm.ApplySchemaUpdates(); err != nil {
		t.Fatal(err)
	}
	return &TestOrm{orm, tc}
}

func TestCreateAndLoadByKey(t *testing.T) {
	orm := NewTestOrm(t)
	defer orm.Close()

	row_in := ormTestType{A: true, C: 1, D: "x"}
	if err := orm.Create(&row_in); err != nil {
		t.Fatal(err)
	}

	var row_out ormTestType
	orm.LoadByKey(&row_out, "x", 1, true)
	if !rowsEqual(&row_in, &row_out) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", row_in, row_out)
	}

	if err := orm.Create(&row_in); err != ErrAlreadyExists {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
	// reconstruct to clear loadedColumns()
	row_in = ormTestType{A: true, C: 1, D: "x"}
	if err := orm.Create(&row_in); err != ErrAlreadyExists {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}
