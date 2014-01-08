package datastore

import "reflect"
import "testing"

import "tux21b.org/v1/gocql"

func TestDefineTable(t *testing.T) {
	expected := []Column{
		Column{Name: "A", Type: "boolean", typeInfo: &gocql.TypeInfo{Type: gocql.TypeBoolean}},
		Column{Name: "B", Type: "double", typeInfo: &gocql.TypeInfo{Type: gocql.TypeDouble}},
		Column{Name: "C", Type: "bigint", typeInfo: &gocql.TypeInfo{Type: gocql.TypeBigInt}},
		Column{Name: "D", Type: "varchar", typeInfo: &gocql.TypeInfo{Type: gocql.TypeVarchar}},
		Column{Name: "E", Type: "timestamp", typeInfo: &gocql.TypeInfo{Type: gocql.TypeTimestamp}},
	}
	s := DefineTable(&ormTestType{}, TableOptions{})
	if !reflect.DeepEqual(expected, s.Columns) {
		t.Errorf("expected %+v, got %+v", expected, s)
	}
}

func TestCreateStatement(t *testing.T) {
	expected := "CREATE TABLE ormTestType (D varchar, C bigint, A boolean, B double, E timestamp, PRIMARY KEY (D, C, A))"
	stmt := ormTestTypeTable.CreateStatement()
	if expected != stmt {
		t.Errorf("\nexpected: %s\nreceived: %s", expected, stmt)
	}
}
