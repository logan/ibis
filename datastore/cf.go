package datastore

import "fmt"
import "reflect"
import "strings"

import "tux21b.org/v1/gocql"

// A ColumnFamily describes how rows of a table are stored in Cassandra.
type ColumnFamily struct {
	Name    string     // The name of the column family.
	Columns []Column   // The definition of the column family's columns.
	Options *CFOptions // Options for the column family, such as primary key.
	orm     *Orm
}

// CreateStatement returns the CQL statement that would create this table.
func (t *ColumnFamily) CreateStatement() string {
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

// A Column gives the name and data type of a Cassandra column. The value of type should be a CQL
// data type (e.g. bigint, varchar, double).
type Column struct {
	Name     string
	Type     string
	typeInfo *gocql.TypeInfo
}

func (cf *ColumnFamily) fillFromRowType(name string, row_type reflect.Type) {
	if row_type.Kind() != reflect.Ptr {
		panic("row must be pointer to struct")
	}
	row_type = row_type.Elem()
	if row_type.Kind() != reflect.Struct {
		panic("row must be pointer to struct")
	}

	cf.Name = strings.ToLower(name)
	cf.Columns = columnsFromStructType(row_type)
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

// Bind returns a new ColumnFamily bound to the given *Orm.
func (t *ColumnFamily) Bind(orm *Orm) {
	t.orm = orm
}

// IsBound returns true if the table is bound to an *Orm.
func (t *ColumnFamily) IsBound() bool {
	return t.orm != nil
}

// IsValidType returns true if the given Row is registered with the column family.
func (t *ColumnFamily) IsValidRowType(row Row) bool {
	return t == row.GetCF()
}

// Exists returns true if a row exists in the table's column family with the given primary key.
func (t *ColumnFamily) Exists(key ...interface{}) (bool, error) {
	if !t.IsBound() {
		return false, ErrTableNotBound
	}
	return t.orm.Exists(t, key...)
}

// LoadByKey loads a row from the table by primary key and stores it in the given Row.
func (t *ColumnFamily) LoadByKey(row Row, key ...interface{}) error {
	if !t.IsBound() {
		return ErrTableNotBound
	}
	if !t.IsValidRowType(row) {
		return ErrInvalidType
	}
	return t.orm.LoadByKey(t, row, key...)
}

// CommitCAS inserts a filled-in "row" into the table's column family. An error is returned if the
// type of the row is not compatible with the one registered for the table, or if a row already
// exists with the same primary key.
func (t *ColumnFamily) CommitCAS(row Row) error {
	if !t.IsBound() {
		return ErrTableNotBound
	}
	if !t.IsValidRowType(row) {
		return ErrInvalidType
	}
	// TODO: handle pk changes
	return t.orm.Commit(t, row, true)
}

// Commit writes any modified values in a loaded "row" to the table's column family. An error is
// returned if the type of the row is not compatible with the one registered for the table.
func (t *ColumnFamily) Commit(row Row) error {
	if !t.IsBound() {
		return ErrTableNotBound
	}
	if !t.IsValidRowType(row) {
		return ErrInvalidType
	}
	return t.orm.Commit(t, row, false)
}

func (t *ColumnFamily) PrepareCommit(row Row) ([]*CQL, error) {
	if !t.IsBound() {
		return nil, ErrTableNotBound
	}
	if !t.IsValidRowType(row) {
		return nil, ErrInvalidType
	}
	return t.orm.PrepareCommit(t, row, false)
}
