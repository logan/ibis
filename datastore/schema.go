package datastore

import "fmt"
import "strings"

type Schema struct {
	Tables map[string]*Table
}

func NewSchema() *Schema {
	return &Schema{make(map[string]*Table)}
}

func (s *Schema) AddTable(table interface{}, options TableOptions) {
	var t *Table
	switch table.(type) {
	default:
		t = GenerateTable(table, options)
	case *Table:
		t = table.(*Table)
		t.Options = options
	}
	s.Tables[strings.ToLower(t.Name)] = t
}

type Table struct {
	Name    string
	Columns []Column
	Options TableOptions
}

func (t Table) CreateStatement() string {
	cols := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		cols[i] = fmt.Sprintf("%s %s", col.Name, col.Type)
	}
	return fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (%s))",
		t.Name, strings.Join(cols, ", "), strings.Join(t.Options.PrimaryKey, ", "))
}

type TableOptions struct {
	PrimaryKey []string
}

type Column struct {
	Name string
	Type string
}
