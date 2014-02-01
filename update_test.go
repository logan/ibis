package ibis

import "reflect"
import "testing"

func TestGetLiveSchema(t *testing.T) {
	tc := NewTestConn(t)
	defer tc.Close()

	schema, err := GetLiveSchema(tc)
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.CFs) > 0 {
		t.Fatalf("expected empty keyspace")
	}

	var b CQLBuilder
	b.Append("CREATE TABLE test (").
		Append("blobcol blob, ").
		Append("boolcol boolean, ").
		Append("float64col double, ").
		Append("int64col bigint, ").
		Append("stringcol varchar, ").
		Append("timecol timestamp, ").
		Append("PRIMARY KEY (stringcol, int64col, boolcol))")
	cql := b.CQL()
	cql.Cluster(tc)
	if err = cql.Query().Exec(); err != nil {
		t.Fatal(err)
	}

	expected := CF{
		name: "test",
		columns: []Column{
			Column{Name: "blobcol", Type: "blob"},
			Column{Name: "boolcol", Type: "boolean"},
			Column{Name: "float64col", Type: "double"},
			Column{Name: "int64col", Type: "bigint"},
			Column{Name: "stringcol", Type: "varchar"},
			Column{Name: "timecol", Type: "timestamp"},
		},
	}
	expected.Key("stringcol", "int64col", "boolcol")
	schema, err = GetLiveSchema(tc)
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
	cluster := NewTestConn(t)
	defer cluster.Close()

	diff, err := DiffLiveSchema(cluster, &Schema{})
	if err != nil {
		t.Fatal(err)
	}
	if diff.Size() != 0 {
		t.Error("expected empty diff")
	}

	model := &Schema{
		CFs: Keyspace{
			"T1": &CF{
				name: "T1",
				columns: []Column{
					Column{Name: "A", Type: "varchar"},
					Column{Name: "B", Type: "varchar"},
				},
			},
		},
		Cluster: cluster,
	}
	model.CFs["T1"].Key("A")

	expected := &SchemaDiff{creations: []*CF{model.CFs["T1"]}}
	diff, err = DiffLiveSchema(cluster, model)
	if err != nil {
		t.Fatal(err)
	}
	if expected.String() != diff.String() {
		t.Errorf("\nexpected: %s\nreceived: %s", expected, diff)
	}

	if err = diff.Apply(cluster); err != nil {
		t.Fatal(err)
	}

	model.CFs["T1"].columns[1].Type = "blob"
	model.CFs["T1"].columns = append(model.CFs["T1"].columns, Column{Name: "C", Type: "bigint"})
	model.CFs["T2"] = &CF{
		name:    "T2",
		columns: []Column{Column{Name: "X", Type: "varchar"}},
	}
	model.CFs["T2"].Key("X")
	expected = &SchemaDiff{
		creations: []*CF{model.CFs["T2"]},
		alterations: []tableAlteration{
			tableAlteration{
				TableName:      "T1",
				NewColumns:     model.CFs["T1"].columns[2:],
				AlteredColumns: []Column{model.CFs["T1"].columns[1]},
			},
		},
	}
	diff, err = DiffLiveSchema(cluster, model)
	if err != nil {
		t.Fatal(err)
	}
	if expected.String() != diff.String() {
		t.Errorf("\nexpected: %s\nreceived: %s", expected, diff)
	}

	if err = diff.Apply(cluster); err != nil {
		t.Fatal(err)
	}
	diff, err = DiffLiveSchema(cluster, &Schema{})
	if err != nil {
		t.Fatal(err)
	}
	if diff.Size() != 0 {
		t.Error("expected empty diff")
	}
}
