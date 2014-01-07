package datastore

import "fmt"
import "reflect"
import "strings"

type Schema struct {
	Tables map[string]*Table
}

func NewSchema() *Schema {
	return &Schema{make(map[string]*Table)}
}

func (s *Schema) AddTable(table *Table) {
	s.Tables[strings.ToLower(table.Name)] = table
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

var ColumnTypeMap = map[string]string{
	"bool":      "boolean",
	"float64":   "double",
	"int64":     "bigint",
	"string":    "varchar",
	"time.Time": "timestamp",
}

func TableFrom(instance interface{}) *Table {
	return tableCache[reflect.TypeOf(instance)]
}

func DefineTable(instance interface{}, options TableOptions) *Table {
	ptr_type := reflect.TypeOf(instance)
	if ptr_type.Kind() != reflect.Ptr {
		panic("instance must be pointer to struct")
	}
	instance_type := reflect.Indirect(reflect.ValueOf(instance)).Type()
	if instance_type.Kind() != reflect.Struct {
		panic("instance must be pointer to struct")
	}
	table := &Table{instance_type.Name(), make([]Column, 0, instance_type.NumField()), options}
	for i := 0; i < instance_type.NumField(); i++ {
		if column, ok := columnFromStructField(instance_type.Field(i)); ok {
			table.Columns = append(table.Columns, column)
		}
	}
	tableCache[ptr_type] = table
	return table
}

func columnFromStructField(field reflect.StructField) (Column, bool) {
	ts, ok := goTypeToCassType(field.Type)
	if ok {
		return Column{field.Name, ts}, true
	}
	return Column{}, ok
}

func goTypeToCassType(t reflect.Type) (string, bool) {
	var type_name string
	if t.PkgPath() == "" {
		type_name = t.Name()
	} else {
		type_name = t.PkgPath() + "." + t.Name()
	}
	result, ok := ColumnTypeMap[type_name]
	return result, ok
}
