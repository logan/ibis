package ibis

import "fmt"
import "reflect"
import "testing"

type row struct {
	Str string `ibis:"key"`
	Int int64
}

type table struct {
	*CF
}

func (t *table) NewCF() *CF {
	t.CF = ReflectCF(row{})
	return t.cf
}

func TestReflectSchema(t *testing.T) {
	type model struct {
		Defined    *CF
		Reflected  *table
		unexported *table // having this here shouldn't break anything
	}

	expectedColumns := []*Column{
		&Column{Name: "Str", Type: "varchar", typeInfo: TIVarchar},
		&Column{Name: "Int", Type: "bigint", typeInfo: TIBigInt},
	}

	m := &model{}
	m.Defined = &CF{columns: expectedColumns}
	m.Defined.SetPrimaryKey("Str")
	schema := ReflectSchema(m)
	expectedPrimaryKey := []string{"Str"}

	checkTable := func(name string) (string, bool) {
		cf, ok := schema.CFs[name]
		if !ok {
			return fmt.Sprintf("column family %#v wasn't included", name), false
		}
		if len(expectedColumns) != len(cf.columns) {
			return fmt.Sprintf("\nexpected columns: %+v\nreceived columns: %+v",
				expectedColumns, cf.columns), false
		}
		for i, exp := range expectedColumns {
			col := cf.columns[i]
			if exp.Name != col.Name || exp.Type != col.Type {
				return fmt.Sprintf("\nexpected column: %+v\nreceived column: %+v",
					*exp, *col), false
			}
		}
		if !reflect.DeepEqual(expectedPrimaryKey, cf.primaryKey) {
			return fmt.Sprintf("\nexpected primary key: %+v\nreceived primary key: %+v",
				expectedColumns, cf.columns), false
		}
		return "", true
	}

	if msg, ok := checkTable("defined"); !ok {
		t.Error(msg)
	}
	if msg, ok := checkTable("reflected"); !ok {
		t.Error(msg)
	}
}
