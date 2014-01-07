package datastore

import "reflect"
import "testing"
import "time"

type TestModel struct {
	A bool
	B float64
	C int64
	D string
	E time.Time
}

func expectPanic(t *testing.T, callable func() interface{}) {
	var result interface{}
	defer func() {
		if recover() == nil {
			t.Errorf("expected panic, got %v", result)
		}
	}()
	result = callable()
}

func TestDefineTable(t *testing.T) {
	expected := []Column{
		Column{"A", "boolean"},
		Column{"B", "double"},
		Column{"C", "bigint"},
		Column{"D", "varchar"},
		Column{"E", "timestamp"},
	}
	s := DefineTable(&TestModel{}, TableOptions{})
	if !reflect.DeepEqual(expected, s.Columns) {
		t.Errorf("expected %+v, got %+v", expected, s)
	}

	expectPanic(t, func() interface{} { return DefineTable(TestModel{}, TableOptions{}) })
	expectPanic(t, func() interface{} { return DefineTable(&expected, TableOptions{}) })
}
