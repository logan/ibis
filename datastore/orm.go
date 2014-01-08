package datastore

import "bytes"
import "errors"
import "fmt"
import "reflect"
import "strings"

import "tux21b.org/v1/gocql"

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
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
	return orm, err
}

// RequiresUpdates returns true if the Orm model differs from the existing column families in
// Cassandra.
func (orm *Orm) RequiresUpdates() bool {
	return orm.SchemaUpdates.Size() > 0
}

// ApplySchemaUpdates applies any required modifications to the live schema to match the Orm model.
func (orm *Orm) ApplySchemaUpdates() error {
	return orm.SchemaUpdates.Apply(orm.Session)
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

func (orm *Orm) commit(row Persistable, ifnotexists bool) error {
	ptr_type := reflect.TypeOf(row)
	table, ok := tableCache[ptr_type]
	if !ok {
		return errors.New(fmt.Sprintf("no table registered for type %v", ptr_type))
	}
	newk, newv, err := getColumnsToCommit(table, row)
	if err != nil {
		return err
	}
	if len(newk) == 0 {
		return nil
	}
	var ifne string
	if ifnotexists {
		ifne = " IF NOT EXISTS"
	}
	loadedColumns := row.loadedColumns()
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)%s",
		table.Name, strings.Join(newk, ", "), placeholderList(len(newk)), ifne)
	params := make([]interface{}, len(newv))
	for i, v := range newv {
		params[i] = v
	}
	q := orm.Query(stmt, params...)
	if ifnotexists {
		savedi := make([]interface{}, len(newk))
		for i, _ := range newk {
			savedi[i] = &RowValue{}
		}
		if applied, err := q.ScanCAS(savedi...); !applied || err != nil {
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
	return nil
}

func getColumnsToCommit(table *Table, row Persistable) (newk []string, newv []RowValue, err error) {
	values, err := getColumnValues(table, row)
	if err != nil {
		return
	}
	newk = make([]string, 0, len(values))
	newv = make([]RowValue, 0, len(values))
	loadedColumns := row.loadedColumns()
	for col, rowval := range values {
		orig, ok := loadedColumns[col]
		if !ok || !bytes.Equal(orig.Value, rowval.Value) {
			newk = append(newk, col)
			newv = append(newv, rowval)
		}
	}
	return
}

func getColumnValues(table *Table, row Persistable) (result RowValues, err error) {
	result = make(RowValues)
	value := reflect.Indirect(reflect.ValueOf(row))
	for _, col := range table.Columns {
		fieldv := value.FieldByName(col.Name)
		if fieldv.IsValid() {
			var b []byte
			b, err = gocql.Marshal(col.typeInfo, fieldv.Interface())
			if err != nil {
				return
			}
			result[col.Name] = RowValue{b, col.typeInfo}
		}
	}
	return
}

// LoadByKey loads data from a row's column family into that row. The row is selected by the given
// key values, which must correspond to the column family's defined primary key.
func (orm *Orm) LoadByKey(row Persistable, key ...interface{}) error {
	ptr_type := reflect.TypeOf(row)
	row_value := reflect.ValueOf(row).Elem()
	table := tableCache[ptr_type]
	pkdef := table.Options.PrimaryKey
	cols := make([]string, len(table.Columns))
	types := make([]*gocql.TypeInfo, len(table.Columns))
	for i, col := range table.Columns {
		cols[i] = col.Name
		types[i] = col.typeInfo
	}
	rules := make([]string, len(pkdef))
	for i, k := range pkdef {
		rules[i] = fmt.Sprintf("%s = ?", k)
	}
	dests := make([]RowValue, len(table.Columns))
	destsi := make([]interface{}, len(table.Columns))
	for i, _ := range table.Columns {
		dests[i] = RowValue{}
		destsi[i] = &dests[i]
	}
	stmt := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(cols, ", "), table.Name, strings.Join(rules, " AND "))
	q := orm.Query(stmt, key...)
	if err := q.Scan(destsi...); err != nil {
		return err
	}
	loadedColumns := row.loadedColumns()
	for i, v := range dests {
		loadedColumns[cols[i]] = v
		gocql.Unmarshal(v.TypeInfo, v.Value, row_value.FieldByName(cols[i]).Addr().Interface())
	}
	return nil
}
