package ibis

import "reflect"
import "testing"

type row struct {
	Str string
	Int int64
}

type table ColumnFamily

func (t *table) ConfigureCF(cf *ColumnFamily) {
	cf.Key("Str")
	cf.Reflect(row{})
}

func TestReflectSchemaFrom(t *testing.T) {
	type model struct {
		T          *table
		unexported *table // having this here shouldn't break anything
	}

	expected := &ColumnFamily{
		Name: "t",
		Columns: []Column{
			Column{Name: "Str", Type: "varchar", typeInfo: TIVarchar},
			Column{Name: "Int", Type: "bigint", typeInfo: TIBigInt},
		},
	}
	expected.Key("Str").Reflect(row{}).typeID = 1

	schema := ReflectSchemaFrom(&model{})
	if !reflect.DeepEqual(expected.Columns, schema.CFs["t"].Columns) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", expected, *schema.CFs["bags"])
	}
}
