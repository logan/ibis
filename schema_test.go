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
	m.Defined = &CF{columns: expectedColumns}
	m.Defined.Key("Str")
	t.Logf("m.Defined.columns = %+v", m.Defined.columns)
	schema := ReflectSchema(m)
	t.Logf("m.Defined.columns = %+v", m.Defined.columns)

	cf, ok := schema.CFs["defined"]
	if !ok {
		t.Error("column family 'defined' wasn't included")
	} else if !reflect.DeepEqual(expectedColumns, cf.columns) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", expectedColumns, cf.columns)
	}

	cf, ok = schema.CFs["reflected"]
	if !ok {
		t.Error("column family 'reflected' wasn't included")
	} else if !reflect.DeepEqual(expectedColumns, cf.columns) {
		t.Errorf("\nexpected: %+v\nreceived: %+v", expectedColumns, cf.columns)
	}
}
