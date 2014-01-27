package ibis

import "reflect"
import "testing"

type row struct {
	Str string
	Int int64
}

type table struct {
	*ColumnFamily
}

func (t *table) CF() *ColumnFamily {
	t.ColumnFamily = ReflectColumnFamily(row{})
	return t.ColumnFamily.Key("Str")
}

func TestReflectSchemaFrom(t *testing.T) {
	type model struct {
		Defined    *ColumnFamily
		Reflected  *table
		unexported *table // having this here shouldn't break anything
	}

	expectedColumns := []Column{
		Column{Name: "Str", Type: "varchar", typeInfo: TIVarchar},
		Column{Name: "Int", Type: "bigint", typeInfo: TIBigInt},
	}
	m := &model{}
	m.Defined = &ColumnFamily{Columns: expectedColumns}
	m.Defined.Key("Str")
	t.Logf("m.Defined.Columns = %+v", m.Defined.Columns)
	schema := ReflectSchemaFrom(m)
	t.Logf("m.Defined.Columns = %+v", m.Defined.Columns)

	cf, ok := schema.CFs["defined"]
	if !ok {
		t.Error("column family 'defined' wasn't included")
	} else if !reflect.DeepEqual(expectedColumns, cf.Columns) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", expectedColumns, cf.Columns)
	}

	cf, ok = schema.CFs["reflected"]
	if !ok {
		t.Error("column family 'reflected' wasn't included")
	} else if !reflect.DeepEqual(expectedColumns, cf.Columns) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", expectedColumns, cf.Columns)
	}
}
