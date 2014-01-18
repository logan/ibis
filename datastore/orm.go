package datastore

import "errors"
import "fmt"
import "reflect"
import "strings"

import "tux21b.org/v1/gocql"

var (
	ErrNotFound           = errors.New("not found")
	ErrAlreadyExists      = errors.New("already exists")
	ErrTableNotAssociated = errors.New("no table associated with this type of row")
)

var tableCache = make(map[reflect.Type]*Table)

// Persistent is an embeddable struct that provides the ability to persist data to Cassandra. For
// example:
//
//   type Page struct {
//     datastore.Persistent
//     Path string
//     Title string
//     Body string
//     Views int64
//     Public bool
//   }
//   var PageTableOptions = datastore.TableOptions{PrimaryKey: []string{"Path"}}
//   var PageTable = datastore.DefineTable(&Page{}, PageTableOptions)
//
// If an empty instance of this struct is passed to DefineTable, then filled in instances of the
// struct can be saved to and loaded from Cassandra.
//
//   page := &Page{Path: "/", Title: "Home Page", Body: "Welcome!", Views: 0, Public: true}
//   orm.Create(page)
//
//   loaded := Page{}
//   orm.LoadByKey(&loaded, "/")
type Persistent struct {
	_loadedColumns RowValues
}

// A Persistable is any struct that embeds Persistent. If such a struct is associated with a Table
// in a Model, then it can be easily persisted to Cassandra.
type Persistable interface {
	loadedColumns() RowValues
}

func (s *Persistent) loadedColumns() RowValues {
	if s._loadedColumns == nil {
		s._loadedColumns = make(RowValues)
	}
	return s._loadedColumns
}

type Orm struct {
	*CassandraConn
	Model         *Schema     // The tables (column families) to use in this keyspace.
	SchemaUpdates *SchemaDiff // The differences found between the existing column families and the given Model.
	SeqID         *SeqIDGenerator
}

// DialOrm establishes a connection to Cassandra and returns an Orm pointer for storing and loading
// Persistables according to a given model.
func DialOrm(config CassandraConfig, model *Schema) (*Orm, error) {
	conn, err := DialCassandra(config)
	if err != nil {
		return nil, err
	}
	orm := &Orm{CassandraConn: conn, Model: model}
	orm.SchemaUpdates, err = DiffLiveSchema(conn, model)
	if err != nil {
		return nil, err
	}
	orm.SeqID, err = NewSeqIDGenerator()
	if err != nil {
		return nil, err
	}
	return orm, nil
}

// RequiresUpdates returns true if the Orm model differs from the existing column families in
// Cassandra.
func (orm *Orm) RequiresUpdates() bool {
	return orm.SchemaUpdates.Size() > 0
}

// ApplySchemaUpdates applies any required modifications to the live schema to match the Orm model.
func (orm *Orm) ApplySchemaUpdates() error {
	return orm.SchemaUpdates.Apply(orm)
}

var placeholderListString string

func init() {
	p := make([]string, 100)
	for i := 0; i < len(p); i++ {
		p[i] = "?"
	}
	placeholderListString = strings.Join(p, ", ")
}

func placeholderList(n int) string {
	return placeholderListString[:3*(n-1)+1]
}

// Create inserts a filled-in "row" into its column family. An error is returned if a row already
// exists with the same primary key.
func (orm *Orm) Create(row Persistable) error {
	if len(row.loadedColumns()) > 0 {
		return ErrAlreadyExists
	}
	return orm.commit(row, true)
}

// Commit writes any modified values in a loaded "row" to its column family.
func (orm *Orm) Commit(row Persistable) error {
	return orm.commit(row, false)
}

func (orm *Orm) commit(row Persistable, cas bool) error {
	ptr_type := reflect.TypeOf(row)
	table, ok := tableCache[ptr_type]
	if !ok {
		return ErrTableNotAssociated
	}
	row_values, err := MarshalRow(row)
	if err != nil {
		return err
	}
	var seqid *SeqID
	if table.Options.IndexBySeqID {
		s, err := orm.SeqID.New()
		if err != nil {
			return err
		}
		seqid = &s
		ti := &gocql.TypeInfo{Type: gocql.TypeVarchar}
		b, err := gocql.Marshal(ti, string(s))
		if err != nil {
			return err
		}
		row_values["SeqID"] = &RowValue{b, ti}
	}
	loadedColumns := row.loadedColumns()
	row_values.subtractUnchanged(loadedColumns)
	newk := make([]string, 0, len(row_values))
	newv := make([]*RowValue, 0, len(row_values))
	for k, v := range row_values {
		newk = append(newk, k)
		newv = append(newv, v)
	}
	if len(newk) == 0 {
		// nothing to commit
		return nil
	}

	stmt := buildInsertStatement(table, newk, cas)
	params := make([]interface{}, len(newv))
	for i, v := range newv {
		params[i] = v
	}

	q := orm.Query(stmt, params...)
	if cas {
		// A CAS query uses ScanCAS for the lightweight transaction. This returns a boolean
		// indicating success, and a row with the values that were committed. We don't need this
		// response, but we need to supply RowValue pointers for the returned columns anyway.
		interfaces := make([]interface{}, len(newk))
		for i, _ := range newk {
			interfaces[i] = &RowValue{}
		}
		if applied, err := q.ScanCAS(interfaces...); !applied || err != nil {
			if err == nil {
				err = ErrAlreadyExists
			}
			return err
		}
	} else {
		if err := q.Exec(); err != nil {
			return err
		}
	}
	for i, col := range newk {
		loadedColumns[col] = newv[i]
	}
	if table.Options.IndexBySeqID {
		if err = addToSeqIDListing(orm, table, *seqid, row_values); err != nil {
			return err
		}
	}
	return nil
}

func buildInsertStatement(table *Table, colnames []string, cas bool) string {
	var cas_term string
	if cas {
		cas_term = " IF NOT EXISTS"
	}
	colname_list := strings.Join(colnames, ", ")
	placeholders := placeholderList(len(colnames))
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)%s", table.Name, colname_list, placeholders, cas_term)
}

// LoadByKey loads data from a row's column family into that row. The row is selected by the given
// key values, which must correspond to the column family's defined primary key.
func (orm *Orm) LoadByKey(row Persistable, key ...interface{}) error {
	ptr_type := reflect.TypeOf(row)
	table, ok := tableCache[ptr_type]
	if !ok {
		return ErrTableNotAssociated
	}

	colnames := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		colnames[i] = col.Name
	}

	pkdef := table.Options.PrimaryKey
	rules := make([]string, len(pkdef))
	for i, k := range pkdef {
		rules[i] = fmt.Sprintf("%s = ?", k)
	}

	stmt := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(colnames, ", "), table.Name, strings.Join(rules, " AND "))
	q := orm.Query(stmt, key...)
	if err := q.Scan(row.loadedColumns().receiverInterfaces(colnames)...); err != nil {
		return err
	}
	return row.loadedColumns().UnmarshalRow(row)
}

// Exists checks if a row exists in a given row's column family.
func (orm *Orm) Exists(row Persistable, key ...interface{}) (bool, error) {
	ptr_type := reflect.TypeOf(row)
	table, ok := tableCache[ptr_type]
	if !ok {
		return false, ErrTableNotAssociated
	}
	pkdef := table.Options.PrimaryKey
	rules := make([]string, len(pkdef))
	for i, k := range pkdef {
		rules[i] = fmt.Sprintf("%s = ?", k)
	}

	stmt := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s",
		table.Name, strings.Join(rules, " AND "))
	q := orm.Query(stmt, key...)
	var count int
	if err := q.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
