package ibis

import "reflect"
import "testing"

type row struct {
	Str string
	Int int64
}

type table struct {
	*CF
}

func (t *table) NewCF() *CF {
	t.CF = ReflectCF(row{})
	return t.cf.Key("Str")
}

func TestReflectSchema(t *testing.T) {
	type model struct {
		Defined    *CF
		Reflected  *table
		unexported *table // having this here shouldn't break anything
	}

	expectedColumns := []Column{
		Column{Name: "Str", Type: "varchar", typeInfo: TIVarchar},
		Column{Name: "Int", Type: "bigint", typeInfo: TIBigInt},
	}
	m := &model{}
	m.Defined = &CF{Columns: expectedColumns}
	m.Defined.Key("Str")
	t.Logf("m.Defined.Columns = %+v", m.Defined.Columns)
	schema := ReflectSchema(m)
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
