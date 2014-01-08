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
	if len(schema.Tables) > 0 {
		t.Fatalf("expected empty keyspace")
	}

	q := tc.Query(`
        CREATE TABLE test (
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

	expected := Table{
		Name: "test",
		Columns: []Column{
			Column{Name: "boolcol", Type: "boolean"},
			Column{Name: "float64col", Type: "double"},
			Column{Name: "int64col", Type: "bigint"},
			Column{Name: "stringcol", Type: "varchar"},
			Column{Name: "timecol", Type: "timestamp"},
		},
		Options: TableOptions{PrimaryKey: []string{"stringcol", "int64col", "boolcol"}},
	}
	schema, err = GetLiveSchema(tc.CassandraConn)
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Tables) != 1 {
		t.Fatalf("expected one table, received %d", len(schema.Tables))
	}
	if !reflect.DeepEqual(expected, *schema.Tables["test"]) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", expected, *schema.Tables["test"])
	}
}

func TestDiffLiveSchema(t *testing.T) {
	tc := NewTestConn(t)
	defer tc.Close()

	diff, err := DiffLiveSchema(tc.CassandraConn, &Schema{})
	if err != nil {
		t.Fatal(err)
	}
	if diff.Size() != 0 {
		t.Error("expected empty diff")
	}

	model := &Schema{
		Tables: map[string]*Table{
			"T1": &Table{
				Name: "T1",
				Columns: []Column{
					Column{Name: "A", Type: "varchar"},
					Column{Name: "B", Type: "boolean"},
				},
				Options: TableOptions{PrimaryKey: []string{"A"}},
			},
		},
	}

	expected := &SchemaDiff{Creations: []*Table{model.Tables["T1"]}}
	diff, err = DiffLiveSchema(tc.CassandraConn, model)
	if err != nil {
		t.Fatal(err)
	}
	if expected.String() != diff.String() {
		t.Errorf("\nexpected: %s\nreceived: %s", expected, diff)
	}

	if err = diff.Apply(tc.Session); err != nil {
		t.Fatal(err)
	}

	model.Tables["T1"].Columns[1].Type = "bigint"
	model.Tables["T2"] = &Table{
		Name:    "T2",
		Columns: []Column{Column{Name: "X", Type: "varchar"}},
		Options: TableOptions{PrimaryKey: []string{"X"}},
	}
	expected = &SchemaDiff{
		Creations: []*Table{model.Tables["T2"]},
		Alterations: []TableAlteration{
			TableAlteration{
				TableName:      "T1",
				NewColumns:     model.Tables["T1"].Columns[2:],
				AlteredColumns: []Column{model.Tables["T1"].Columns[1]},
			},
		},
	}
	diff, err = DiffLiveSchema(tc.CassandraConn, model)
	if err != nil {
		t.Fatal(err)
	}
	if expected.String() != diff.String() {
		t.Errorf("\nexpected: %s\nreceived: %s", expected, diff)
	}
}
