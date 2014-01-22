package datastore

import "fmt"
import "testing"

func TestCQL(t *testing.T) {
	expect := func(exp string, sel *CQL) (string, bool) {
		rcv := sel.String()
		return fmt.Sprintf("\nexpected: '%s'\nreceived: '%s'", exp, rcv), exp == rcv
	}

	cf := &ColumnFamily{
		Name: "Table",
		Columns: []Column{
			Column{Name: "X"},
			Column{Name: "Y"},
			Column{Name: "Z"},
		},
	}
	if msg, ok := expect("SELECT X, Y, Z FROM Table", NewSelect(cf)); !ok {
		t.Errorf(msg)
	}
	if msg, ok := expect("SELECT X, Y, Z FROM Table", NewSelect(cf).Cols("*")); !ok {
		t.Errorf(msg)
	}
}
