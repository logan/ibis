package datastore

import "reflect"
import "testing"

func TestGetLiveSchema(t *testing.T) {
	tc := NewTestConn(t)
	defer tc.Close()

	schema, err := GetLiveSchema(tc.CassandraConn)
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.CFs) > 0 {
		t.Fatalf("expected empty keyspace")
	}

	q := tc.Query(`
        CREATE TABLE test (
            blobcol blob,
            boolcol boolean,
            float64col double,
            int64col bigint,
            stringcol varchar,
            timecol timestamp,
            PRIMARY KEY (stringcol, int64col, boolcol)
        )`)
	if err = q.Exec(); err != nil {
		t.Fatal(err)
	}

	expected := ColumnFamily{
		Name: "test",
		Columns: []Column{
			Column{Name: "blobcol", Type: "blob"},
			Column{Name: "boolcol", Type: "boolean"},
			Column{Name: "float64col", Type: "double"},
			Column{Name: "int64col", Type: "bigint"},
			Column{Name: "stringcol", Type: "varchar"},
			Column{Name: "timecol", Type: "timestamp"},
		},
	}
	expected.Options = NewCFOptions(&expected).Key("stringcol", "int64col", "boolcol")
	schema, err = GetLiveSchema(tc.CassandraConn)
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.CFs) != 1 {
		t.Fatalf("expected one table, received %d", len(schema.CFs))
	}
	if !reflect.DeepEqual(expected, *schema.CFs["test"]) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", expected, *schema.CFs["test"])
	}
}

func TestDiffLiveSchema(t *testing.T) {
	orm := NewTestOrm(t)
	defer orm.Close()

	diff, err := DiffLiveSchema(orm.TestConn.CassandraConn, &Schema{})
	if err != nil {
		t.Fatal(err)
	}
	if diff.Size() != 0 {
		t.Error("expected empty diff")
	}

	model := &Schema{
		CFs: Keyspace{
			"T1": &ColumnFamily{
				Name: "T1",
				Columns: []Column{
					Column{Name: "A", Type: "varchar"},
					Column{Name: "B", Type: "varchar"},
				},
			},
		},
	}
	model.CFs["T1"].Options = NewCFOptions(model.CFs["T1"]).Key("A")

	expected := &SchemaDiff{Creations: []*ColumnFamily{model.CFs["T1"]}}
	diff, err = DiffLiveSchema(orm.TestConn.CassandraConn, model)
	if err != nil {
		t.Fatal(err)
	}
	if expected.String() != diff.String() {
		t.Errorf("\nexpected: %s\nreceived: %s", expected, diff)
	}

	if err = diff.Apply(orm.Orm); err != nil {
		t.Fatal(err)
	}

	model.CFs["T1"].Columns[1].Type = "blob"
	model.CFs["T1"].Columns = append(model.CFs["T1"].Columns, Column{Name: "C", Type: "bigint"})
	model.CFs["T2"] = &ColumnFamily{
		Name:    "T2",
		Columns: []Column{Column{Name: "X", Type: "varchar"}},
	}
	model.CFs["T2"].Options = NewCFOptions(model.CFs["T2"]).Key("X")
	expected = &SchemaDiff{
		Creations: []*ColumnFamily{model.CFs["T2"]},
		Alterations: []TableAlteration{
			TableAlteration{
				TableName:      "T1",
				NewColumns:     model.CFs["T1"].Columns[2:],
				AlteredColumns: []Column{model.CFs["T1"].Columns[1]},
			},
		},
	}
	diff, err = DiffLiveSchema(orm.TestConn.CassandraConn, model)
	if err != nil {
		t.Fatal(err)
	}
	if expected.String() != diff.String() {
		t.Errorf("\nexpected: %s\nreceived: %s", expected, diff)
	}

	if err = diff.Apply(orm.Orm); err != nil {
		t.Fatal(err)
	}
	diff, err = DiffLiveSchema(orm.TestConn.CassandraConn, &Schema{})
	if err != nil {
		t.Fatal(err)
	}
	if diff.Size() != 0 {
		t.Error("expected empty diff")
	}
}
