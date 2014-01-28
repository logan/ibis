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

// CFProvider is an interface for producing and configuring a column family definition. Use
// CFProvider when specifying a schema struct for ReflectSchema(). Exported fields that implement
// this interface will be included in the resulting schema.
type CFProvider interface {
	CF() *ColumnFamily
}

// A ColumnFamily describes how rows of a table are stored in Cassandra. If added to a Schema that
// is connected to a Cluster, then operations on the corresponding column family in the cluster may
// be made by calling ColumnFamily methods like LoadByKey, Commit, etc.
type ColumnFamily struct {
	Name       string   // The name of the column family.
	Columns    []Column // The definition of the column family's columns.
	PrimaryKey []string
	SeqIDGenerator
	*Schema

	typeID int
	*rowReflector
}

// CF returns a pointer to the column family it's called on. This is so *ColumnFamily implements
// the CFProvider interface, allowing CF definitions to be given in a reflected schema.
func (cf *ColumnFamily) CF() *ColumnFamily {
	return cf
}

// Cluster returns an object implementing the Cluster interface, or nil if this column family has
// not been added to a schema that is connected to a cluster.
func (cf *ColumnFamily) Cluster() Cluster {
	if cf.Schema == nil {
		return nil
	}
	return cf.Schema.Cluster
}

// Key configures the primary key for this column family. It takes names of columns as strings.
// At least one argument is required, specifying the partition key. Zero or more additional
// arguments specify the name of the clustering columns.
//
// Key returns a pointer to the column family it was called on so it can be chained during
// configuration.
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
	Type     string // The cassandra type of the column ("varchar", "bigint", etc.).
	typeInfo *gocql.TypeInfo
}

func (cf *ColumnFamily) fillFromRowType(row_type reflect.Type) {
	if row_type.Kind() != reflect.Struct {
		panic("row must be struct")
	}
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

// IsBound returns true if the column family is part of a schema that is connected to a cluster.
func (cf *ColumnFamily) IsBound() bool {
	return cf.Cluster() != nil
}

// Exists returns true if a row can be found in the column family with the given primary key.
// The values for the key must be given in order respective to the primary key definition for this
// column family (see the Key function).
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

// LoadByKey looks up a row from the table by primary key and stores its value in the given address.
// The dest argument should implement the Row interface. Alternatively, if this column family was
// generated by reflection, then the dest argument may be a pointer to a value of the same type that
// was reflected.
//
// The values for the key must be given in order respective to the primary key definition for this
// column family (see the Key function). If no row is found under the given key, ErrNotFound is
// returned.
func (cf *ColumnFamily) LoadByKey(dest interface{}, key ...interface{}) error {
	if !cf.IsBound() {
		return ErrTableNotBound
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
		if err := qiter.Close(); err != nil {
			return err
		}
		return ErrNotFound
	}
	return cf.unmarshal(dest, mmap)
}

// CommitCAS writes a row to the column family if no row already exists under the same key. If a row
// with the same key already exists, ErrAlreadyExists will be returned.
//
// The row argument should implement the Row interface. Alternatively, if this column family was
// generated by reflection, then the row argument may be a pointer to a value of the same type that
// was reflected.
func (cf *ColumnFamily) CommitCAS(row interface{}) error {
	if !cf.IsBound() {
		return ErrTableNotBound
	}
	// TODO: handle pk changes
	return cf.commit(row, true)
}

// Commit writes a row to the column family. If a row already exists with the same key, it will be
// overwritten.
//
// The row argument should implement the Row interface. Alternatively, if this column family was
// generated by reflection, then the row argument may be a pointer to a value of the same type that
// was reflected.
func (cf *ColumnFamily) Commit(row interface{}) error {
	if !cf.IsBound() {
		return ErrTableNotBound
	}
	// TODO: handle pk changes
	return cf.commit(row, false)
}

func (cf *ColumnFamily) generateCommit(mmap MarshalledMap, cas bool) (cql CQL, ok bool) {
	// Generate the appropriate CQL.
	selectedKeys := make([]string, len(cf.Columns))
	for i, col := range cf.Columns {
		selectedKeys[i] = col.Name
	}
	if cas {
		ins := InsertInto(cf).
			Keys(selectedKeys...).
			Values(mmap.InterfacesFor(selectedKeys...)...).
			IfNotExists()
		cql = ins.CQL()
		ok = true
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
			ok = true
		}
	}
	return
}

func (cf *ColumnFamily) commit(row interface{}, cas bool) error {
	mmap, err := cf.marshal(row)
	if err != nil {
		return err
	}

	cql, ok := cf.generateCommit(mmap, cas)
	if !ok {
		return nil
	}

	// Apply the INSERT or UPDATE and check results.
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
	return cf.unmarshal(row, mmap)
}

func (cf *ColumnFamily) marshal(src interface{}) (MarshalledMap, error) {
	row, ok := src.(Row)
	if !ok {
		if cf.rowReflector == nil {
			return nil, ErrInvalidType
		}
		var err error
		row, err = cf.rowReflector.reflectedRow(src)
		if err != nil {
			return nil, err
		}
	}
	mmap := make(MarshalledMap)
	if err := row.Marshal(mmap); err != nil {
		return nil, err
	}
	return mmap, nil
}

func (cf *ColumnFamily) unmarshal(dest interface{}, mmap MarshalledMap) error {
	row, ok := dest.(Row)
	if !ok {
		if cf.rowReflector == nil {
			return ErrInvalidType
		}
		var err error
		row, err = cf.rowReflector.reflectedRow(dest)
		if err != nil {
			return err
		}
	}
	return row.Unmarshal(mmap)
}

// ReflectColumnFamily generates a column family definition from a struct value. It uses reflection
// to inspect the fields of the given template (which can be a zero value). A column is configured
// for each exported field that has a marshalable type. Currently supported types are:
//
//  * string     (marshals to varchar)
//  * ibis.SeqID (marshals to varchar, filled in automatically when a SeqIDGenerator is configured)
//  * []byte     (marshals to blob)
//  * int64      (marshals to bigint)
//  * float64    (marshals to double)
//  * bool       (marshals to boolean)
//  * time.Time  (marshals to timestamp)
//
// The returned ColumnFamily will support row operations on pointers to values of the same type as
// the given template, without requiring an implementation of the Row interface.
func ReflectColumnFamily(template interface{}) *ColumnFamily {
	cf := &ColumnFamily{}
	cf.rowReflector = newRowReflector(cf, template)
	return cf
}