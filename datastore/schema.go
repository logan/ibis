package datastore

import "fmt"
import "reflect"
import "strings"

import "tux21b.org/v1/gocql"

// Schema is a collection of Table (column family) definitions.
type Schema struct {
	Tables     map[string]*Table
	nextTypeID int
}

// NewSchema returns a new, empty schema.
func NewSchema() *Schema {
	return &Schema{Tables: make(map[string]*Table)}
}

// AddTable adds a table definition to the schema.
func (s *Schema) AddTable(table *Table) {
	s.Tables[strings.ToLower(table.Name)] = table
	if table.seqIDTable != nil {
		s.Tables[table.seqIDTable.Name] = table.seqIDTable
	}
	if table.Options.typeID == 0 {
		table.Options.typeID = s.nextTypeID
		s.nextTypeID++
	}
}

// A Table describes a column family in Cassandra.
type Table struct {
	Name       string       // The name of the column family.
	Columns    []Column     // The definition of the column family's columns.
	Options    TableOptions // Options for the column family, such as primary key.
	seqIDTable *Table
}

// CreateStatement returns the CQL statement that would create this table.
func (t Table) CreateStatement() string {
	cols := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		cols[i] = fmt.Sprintf("%s %s", col.Name, col.Type)
	}
	var options string
	if t.Options.typeID != 0 {
		options = fmt.Sprintf(" WITH comment='%d'", t.Options.typeID)
	}
	return fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (%s))%s",
		t.Name, strings.Join(cols, ", "), strings.Join(t.Options.PrimaryKey, ", "), options)
}

type OnCreateHook func(*Orm, *Table) error

// TableOptions is used to provide additional properties for a column family definition.
type TableOptions struct {
	PrimaryKey   []string     // Required. The list of columns comprising the primary key. The first column defines partitions.
	IndexBySeqID bool         // If true, maintain a secondary index of rows by SeqID.
	OnCreate     OnCreateHook // If given, will be called immediately after table creation.
	typeID       int
}

// A Column gives the name and data type of a Cassandra column. The value of type should be a CQL
// data type (e.g. bigint, varchar, double).
type Column struct {
	Name     string
	Type     string
	typeInfo *gocql.TypeInfo
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

	colmap := make(map[string]Column)
	for _, col := range columnsFromStructType(row_type) {
		colmap[col.Name] = col
	}

	table := &Table{
		Name:    row_type.Name(),
		Columns: make([]Column, 0, len(colmap)),
		Options: options,
	}

	// primary key columns must come first and in order
	for _, pk_name := range options.PrimaryKey {
		col, ok := colmap[pk_name]
		if !ok {
			panic(fmt.Sprintf("primary key refers to invalid column (%s)", pk_name))
		}
		table.Columns = append(table.Columns, col)
		delete(colmap, pk_name)
	}
	for _, col := range colmap {
		table.Columns = append(table.Columns, col)
	}

	tableCache[ptr_type] = table

	if options.IndexBySeqID {
		table.seqIDTable = SeqIDListingTable(table)
	}
	return table
}

func columnsFromStructType(struct_type reflect.Type) []Column {
	cols := make([]Column, 0, struct_type.NumField())
	for i := 0; i < struct_type.NumField(); i++ {
		field := struct_type.Field(i)
		if col, ok := columnFromStructField(field); ok {
			cols = append(cols, col)
		} else if field.Type.Kind() == reflect.Struct {
			cols = append(cols, columnsFromStructType(field.Type)...)
		}
	}
	return cols
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
	if t.Kind() == reflect.Slice {
		if t.Elem().Kind() == reflect.Uint8 {
			type_name = "[]byte"
		}
	} else if t.PkgPath() == "" {
		type_name = t.Name()
	} else {
		type_name = t.PkgPath() + "." + t.Name()
	}
	result, ok := columnTypeMap[type_name]
	return result, ok
}
