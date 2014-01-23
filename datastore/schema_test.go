package datastore

import "reflect"
import "testing"

func TestReflectSchemaFrom(t *testing.T) {
	expected := ColumnFamily{
		Name: "bags",
		Columns: []Column{
			Column{Name: "D", Type: "varchar", typeInfo: tiVarchar},
			Column{Name: "C", Type: "bigint", typeInfo: tiBigInt},
			Column{Name: "A", Type: "boolean", typeInfo: tiBoolean},
			Column{Name: "B", Type: "double", typeInfo: tiDouble},
			Column{Name: "E", Type: "timestamp", typeInfo: tiTimestamp},
			Column{Name: "F", Type: "blob", typeInfo: tiBlob},
		},
	}
	expected.Options = NewCFOptions(&expected)
	bomtt := &bagOfManyTypesTable{}
	bomtt.ConfigureCF(expected.Options)
	expected.Options.typeID = 1

	model := &testModel{}
	schema := ReflectSchemaFrom(model)
	if !reflect.DeepEqual(expected.Columns, schema.CFs["bags"].Columns) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", expected, *schema.CFs["bags"])
	}
}
