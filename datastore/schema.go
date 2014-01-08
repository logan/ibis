package datastore

import "fmt"
import "reflect"
import "strings"

import "tux21b.org/v1/gocql"

// Schema is a collection of Table (column family) definitions.
type Schema struct {
	Tables map[string]*Table
}

// NewSchema returns a new, empty schema.
func NewSchema() *Schema {
	return &Schema{make(map[string]*Table)}
}

// AddTable adds a table definition to the schema.
func (s *Schema) AddTable(table *Table) {
	s.Tables[strings.ToLower(table.Name)] = table
}

// A Table describes a column family in Cassandra.
type Table struct {
	Name    string       // The name of the column family.
	Columns []Column     // The definition of the column family's columns.
	Options TableOptions // Options for the column family, such as primary key.
}

// CreateStatement returns the CQL statement that would create this table.
func (t Table) CreateStatement() string {
	cols := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		cols[i] = fmt.Sprintf("%s %s", col.Name, col.Type)
	}
	return fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (%s))",
		t.Name, strings.Join(cols, ", "), strings.Join(t.Options.PrimaryKey, ", "))
}

// TableOptions is used to provide additional properties for a column family definition.
type TableOptions struct {
	PrimaryKey []string // Required. The list of columns comprising the primary key. The first column defines partitions.
}

// A Column gives the name and data type of a Cassandra column. The value of type should be a CQL
// data type (e.g. bigint, varchar, double).
type Column struct {
	Name     string
	Type     string
	typeInfo *gocql.TypeInfo
}

var columnTypeMap = map[string]string{
	"bool":      "boolean",
	"float64":   "double",
	"int64":     "bigint",
	"string":    "varchar",
	"time.Time": "timestamp",
}

var typeInfoMap = map[string]*gocql.TypeInfo{
	"boolean":   &gocql.TypeInfo{Type: gocql.TypeBoolean},
	"double":    &gocql.TypeInfo{Type: gocql.TypeDouble},
	"bigint":    &gocql.TypeInfo{Type: gocql.TypeBigInt},
	"varchar":   &gocql.TypeInfo{Type: gocql.TypeVarchar},
	"timestamp": &gocql.TypeInfo{Type: gocql.TypeTimestamp},
}

// TableFrom looks up a row's Table definition. Returns nil if no Table has been defined for the row's type.
func TableFrom(row Persistable) *Table {
	return tableCache[reflect.TypeOf(row)]
}

// DefineTable generates a Table definition from a sample row. The row can be entirely empty. The
// reflect package is used to inspect the row's data type and generate the column definitions
// according to its struct fields. Only fields of types that can be mapped to CQL and back are
// considered.
func DefineTable(row Persistable, options TableOptions) *Table {
	ptr_type := reflect.TypeOf(row)
	if ptr_type.Kind() != reflect.Ptr {
		panic("row must be pointer to struct")
	}
	row_type := reflect.Indirect(reflect.ValueOf(row)).Type()
	if row_type.Kind() != reflect.Struct {
		panic("row must be pointer to struct")
	}

	cols := make(map[string]Column)
	for i := 0; i < row_type.NumField(); i++ {
		if col, ok := columnFromStructField(row_type.Field(i)); ok {
			cols[col.Name] = col
		}
	}

	table := &Table{row_type.Name(), make([]Column, 0, len(cols)), options}

	// primary key columns must come first and in order
	for _, pk_name := range options.PrimaryKey {
		col, ok := cols[pk_name]
		if !ok {
			panic(fmt.Sprintf("primary key refers to invalid column (%s)", pk_name))
		}
		table.Columns = append(table.Columns, col)
		delete(cols, pk_name)
	}
	for _, col := range cols {
		table.Columns = append(table.Columns, col)
	}

	tableCache[ptr_type] = table
	return table
}

func columnFromStructField(field reflect.StructField) (Column, bool) {
	ts, ok := goTypeToCassType(field.Type)
	if ok {
		return Column{field.Name, ts, typeInfoMap[ts]}, true
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
	result, ok := columnTypeMap[type_name]
	return result, ok
}
