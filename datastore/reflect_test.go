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

func TestGenerateColumnDefinitions(t *testing.T) {
	expected := []string{
		"A boolean",
		"B double",
		"C bigint",
		"D varchar",
		"E timestamp",
	}
	s := GenerateColumnDefinitions(TestModel{})
	if !reflect.DeepEqual(expected, s) {
		t.Errorf("expected %+v, got %+v", expected, s)
	}
	defer func() {
		if recover() == nil {
			t.Errorf("expected panic, got %v", s)
		}
	}()
	s = GenerateColumnDefinitions(0)
}
