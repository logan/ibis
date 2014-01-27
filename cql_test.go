package ibis

import "fmt"
import "testing"

func TestCQL(t *testing.T) {
	expect := func(exp string, sel CQL) (string, bool) {
		return fmt.Sprintf("\nexpected: '%s'\nreceived: '%s'", exp, sel), exp == sel.String()
	}

	cf := &ColumnFamily{
		Name: "Table",
		Columns: []Column{
			Column{Name: "X"},
			Column{Name: "Y"},
			Column{Name: "Z"},
		},
	}
	if msg, ok := expect("SELECT X, Y, Z FROM Table", Select().From(cf).CQL()); !ok {
		t.Errorf(msg)
	}
	if msg, ok := expect("SELECT X, Y, Z FROM Table", Select("*").From(cf).CQL()); !ok {
		t.Errorf(msg)
	}
}
