package datastore

import "reflect"
import "testing"

type row struct {
	ReflectedRow
	Str string
	Int int64
}

type table ColumnFamily

func (t *table) ConfigureCF(cf *ColumnFamily) {
	cf.Key("Str")
}

func (t *table) NewRow() Row {
	row := &row{}
	row.CF = (*ColumnFamily)(t)
	return row
}

func TestReflectSchemaFrom(t *testing.T) {
	type model struct {
		T          *table
		unexported *table // having this here shouldn't break anything
	}

	expected := &ColumnFamily{
		Name: "t",
		Columns: []Column{
			Column{Name: "Str", Type: "varchar", typeInfo: tiVarchar},
			Column{Name: "Int", Type: "bigint", typeInfo: tiBigInt},
		},
	}
	expected.Key("Str").typeID = 1

	schema := ReflectSchemaFrom(&model{})
	if !reflect.DeepEqual(expected.Columns, schema.CFs["t"].Columns) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", expected, *schema.CFs["bags"])
	}
}
