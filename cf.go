package ibis

import "errors"
import "fmt"
import "reflect"
import "strings"

import "tux21b.org/v1/gocql"

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrTableNotBound = errors.New("table not connected to a cluster")
	ErrInvalidType   = errors.New("invalid row type")
)

type OnCreateHook func(*ColumnFamily) error

// A ColumnFamily describes how rows of a table are stored in Cassandra.
type ColumnFamily struct {
	Name       string   // The name of the column family.
	Columns    []Column // The definition of the column family's columns.
	PrimaryKey []string
	SeqIDGenerator
	*Schema

	onCreateHooks []OnCreateHook
	typeID        int
}

func (cf *ColumnFamily) Cluster() Cluster {
	if cf.Schema == nil {
		return nil
	}
	return cf.Schema.Cluster
}

func (cf *ColumnFamily) Key(keys ...string) *ColumnFamily {
	cf.PrimaryKey = keys

	// primary key columns must come first and in order
	rearranged := make([]Column, len(cf.Columns))
	keymap := make(map[string]bool)
	for i, k := range keys {
		for _, col := range cf.Columns {
			if k == col.Name {
				keymap[k] = true
				rearranged[i] = col
				break
			}
		}
	}
	i := len(keys)
	for _, col := range cf.Columns {
		if _, ok := keymap[col.Name]; !ok {
			rearranged[i] = col
			i++
		}
	}
	copy(cf.Columns, rearranged)
	return cf
}

func (cf *ColumnFamily) OnCreate(hook OnCreateHook) *ColumnFamily {
	cf.onCreateHooks = append(cf.onCreateHooks, hook)
	return cf
}

// CreateStatement returns the CQL statement that would create this table.
func (t *ColumnFamily) CreateStatement() CQL {
	var b CQLBuilder
	b.Append("CREATE TABLE " + t.Name + " (")
	for _, col := range t.Columns {
		b.Append(col.Name + " " + col.Type + ", ")
	}
	b.Append("PRIMARY KEY (" + strings.Join(t.PrimaryKey, ", ") + "))")
	if t.typeID != 0 {
		b.Append(fmt.Sprintf(" WITH comment='%d'", t.typeID))
	}
	cql := b.CQL()
	cql.Cluster(t.Cluster())
	return cql
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

// IsBound returns true if the table is bound to an *Orm.
func (cf *ColumnFamily) IsBound() bool {
	return cf.Cluster() != nil
}

// IsValidType returns true if the given Row is registered with the column family.
func (cf *ColumnFamily) IsValidRowType(row Row) bool {
	return cf == row.GetCF()
}

// Exists returns true if a row exists in the table's column family with the given primary key.
func (cf *ColumnFamily) Exists(key ...interface{}) (bool, error) {
	if !cf.IsBound() {
		return false, ErrTableNotBound
	}
	sel := Select("COUNT(*)").From(cf)
	for i, k := range cf.PrimaryKey {
		sel.Where(k+" = ?", key[i])
	}
	qiter := sel.CQL().Query()
	var count int
	if !qiter.Scan(&count) {
		return false, qiter.Close()
	}
	return count > 0, nil
}

// LoadByKey loads a row from the table by primary key and stores it in the given Row.
func (cf *ColumnFamily) LoadByKey(row Row, key ...interface{}) error {
	if !cf.IsBound() {
		return ErrTableNotBound
	}

	if !cf.IsValidRowType(row) {
		return ErrInvalidType
	}

	colnames := make([]string, len(cf.Columns))
	for i, col := range cf.Columns {
		colnames[i] = col.Name
	}

	sel := Select(colnames...).From(cf)
	for i, k := range cf.PrimaryKey {
		sel.Where(k+" = ?", key[i])
	}
	qiter := sel.CQL().Query()
	mmap := make(MarshalledMap)
	if !qiter.Scan(mmap.PointersTo(colnames...)...) {
		return qiter.Close()
	}
	return row.Unmarshal(mmap)
}

// CommitCAS inserts a filled-in "row" into the table's column family. An error is returned if the
// type of the row is not compatible with the one registered for the table, or if a row already
// exists with the same primary key.
func (cf *ColumnFamily) CommitCAS(row Row) error {
	if !cf.IsBound() {
		return ErrTableNotBound
	}
	if !cf.IsValidRowType(row) {
		return ErrInvalidType
	}
	// TODO: handle pk changes
	return cf.Commit(row, true)
}

func (cf *ColumnFamily) PrepareCommit(row Row, cas bool) ([]CQL, error) {
	if !cf.IsBound() {
		return nil, ErrTableNotBound
	}
	if !cf.IsValidRowType(row) {
		return nil, ErrInvalidType
	}
	mmap := make(MarshalledMap)
	if err := row.Marshal(mmap); err != nil {
		return nil, err
	}
	return cf.precommit(mmap, cas)
}

func (cf *ColumnFamily) precommit(mmap MarshalledMap, cas bool) ([]CQL, error) {
	stmts := make([]CQL, 0, 1)

	// Generate the appropriate CQL.
	selectedKeys := make([]string, len(cf.Columns))
	for i, col := range cf.Columns {
		selectedKeys[i] = col.Name
	}
	var cql CQL
	if cas {
		ins := InsertInto(cf).
			Keys(selectedKeys...).
			Values(mmap.InterfacesFor(selectedKeys...)...).
			IfNotExists()
		cql = ins.CQL()
	} else {
		// For an update, it makes no sense to treat primary keys as dirty.
		for _, k := range cf.PrimaryKey {
			mmap[k].Dirty = false
		}
		selectedKeys = mmap.DirtyKeys()
		if len(selectedKeys) > 0 {
			upd := Update(cf)
			for _, k := range selectedKeys {
				upd.Set(k, mmap[k])
			}
			for _, k := range cf.PrimaryKey {
				upd.Where(k+" = ?", mmap[k])
			}
			cql = upd.CQL()
		}
	}
	return append(stmts, cql), nil
}

// Commit writes any modified values in the given row to the given CF.
func (cf *ColumnFamily) Commit(row Row, cas bool) error {
	mmap := make(MarshalledMap)
	if err := row.Marshal(mmap); err != nil {
		return err
	}

	// TODO: make the separation between precommit and commit stronger
	stmts, err := cf.precommit(mmap, cas)
	if err != nil {
		return err
	}

	// Apply all but the final statement as a batch (as these should be index updates).
	if len(stmts) > 1 {
		qiter := cf.Query(stmts[:len(stmts)-1]...)
		if err = qiter.Close(); err != nil {
			return err
		}
	}

	// Apply the INSERT or UPDATE and check results.
	cql := stmts[len(stmts)-1]
	qiter := cql.Query()
	if cas {
		// A CAS query uses ScanCAS for the lightweight transaction. This returns a boolean
		// indicating success, and a row with the values that were committed. We don't need this
		// response, but we need to supply MarshalledValue pointers for the returned columns anyway.
		// Despite this, the values pointed to will not be filled in except in the case of error.
		casmap := make(MarshalledMap)
		selectedKeys := make([]string, len(cf.Columns))
		for i, col := range cf.Columns {
			selectedKeys[i] = col.Name
		}
		pointers := casmap.PointersTo(selectedKeys...)
		if applied := qiter.ScanCAS(pointers...); !applied {
			err := qiter.Close()
			if err == nil {
				err = ErrAlreadyExists
			}
			return err
		}
	} else {
		if err := qiter.Exec(); err != nil {
			return err
		}
	}

	// Make the row unmarshal its given values, in case it is caching upon load.
	return row.Unmarshal(mmap)
}
