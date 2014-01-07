package datastore

import "fmt"
import "reflect"
import "testing"

func TestGetLiveSchema(t *testing.T) {
	fmt.Println("newtestconn")
	tc, err := NewTestConn()
	if err != nil {
		t.Error(err)
	}
	defer tc.Close()

	schema, err := GetLiveSchema(tc.CassandraConn)
	if err != nil {
		t.Error(err)
	}
	if len(schema.Tables) > 0 {
		t.Errorf("expected empty keyspace")
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
		t.Error(err)
	}

	expected := Table{
		Name: "test",
		Columns: []Column{
			Column{"boolcol", "boolean"},
			Column{"float64col", "double"},
			Column{"int64col", "bigint"},
			Column{"stringcol", "varchar"},
			Column{"timecol", "timestamp"},
		},
		Options: TableOptions{PrimaryKey: []string{"stringcol", "int64col", "boolcol"}},
	}
	schema, err = GetLiveSchema(tc.CassandraConn)
	if err != nil {
		t.Error(err)
	}
	if len(schema.Tables) != 1 {
		t.Errorf("expected one table, received %d", len(schema.Tables))
	}
	if !reflect.DeepEqual(expected, *schema.Tables["test"]) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", expected, *schema.Tables["test"])
	}
}
