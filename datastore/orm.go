package datastore

import "errors"
import "fmt"
import "strings"

import "tux21b.org/v1/gocql"

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrTableNotBound = errors.New("table not bound; call table.Bind(orm)")
	ErrInvalidType   = errors.New("invalid row type")
)

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
//   var PageCFOptions = datastore.CFOptions{PrimaryKey: []string{"Path"}}
//   var PageTable = datastore.DefineTable(&Page{}, PageCFOptions)
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
	CF             *ColumnFamily `json:"-"`
	_loadedColumns RowValues
}

// A Persistable is any struct that embeds Persistent. If such a struct is associated with a Table
// in a Model, then it can be easily persisted to Cassandra.
type Persistable interface {
	GetCF() *ColumnFamily
	loadedColumns() RowValues
}

func (s *Persistent) GetCF() *ColumnFamily {
	return s.CF
}

func (s *Persistent) loadedColumns() RowValues {
	if s._loadedColumns == nil {
		s._loadedColumns = make(RowValues)
	}
	return s._loadedColumns
}

// An Orm binds a Schema to a CassandraConn.
type Orm struct {
	*CassandraConn
	Model         *Schema     // The column families to use in this keyspace.
	SchemaUpdates *SchemaDiff // The differences found between the existing column families and the given Model.
	SeqID         SeqIDGenerator
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

// Commit writes any modified values in the given row to the given CF.
func (orm *Orm) Commit(cf *ColumnFamily, row Persistable, cas bool) error {
	pkdef := cf.Options.PrimaryKey
	row_values, err := MarshalRow(row)
	if err != nil {
		return err
	}
	var seqid *SeqID
	if cf.Options.IndexBySeqID {
		seqid_rv, ok := row_values["SeqID"]
		if !ok || len(seqid_rv.Value) == 0 {
			s, err := orm.SeqID.New()
			if err != nil {
				return err
			}
			seqid = &s
			b, err := gocql.Marshal(tiVarchar, string(s))
			if err != nil {
				return err
			}
			row_values["SeqID"] = &RowValue{b, tiVarchar}
		} else {
			seqid = new(SeqID)
			gocql.Unmarshal(tiVarchar, row_values["SeqID"].Value, seqid)
		}
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

	var stmt string
	var params []interface{}
	if cas {
		stmt = buildInsertStatement(cf, newk)
		params = make([]interface{}, len(newv))
		for i, v := range newv {
			params[i] = v
		}
	} else {
		stmt = buildUpdateStatement(cf, newk)
		pkvals := make([]interface{}, len(pkdef))
		for i, k := range pkdef {
			pkvals[i] = loadedColumns[k]
			delete(row_values, k)
		}
		params = make([]interface{}, len(newv)+len(pkvals))
		for i, v := range newv {
			params[i] = v
		}
		copy(params[len(newv):], pkvals)
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
	if cf.Options.IndexBySeqID {
		if err = addToSeqIDListing(orm, cf, *seqid, row_values); err != nil {
			return err
		}
	}
	return row.loadedColumns().UnmarshalRow(row)
}

func buildInsertStatement(cf *ColumnFamily, colnames []string) string {
	colname_list := strings.Join(colnames, ", ")
	placeholders := placeholderList(len(colnames))
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) IF NOT EXISTS",
		cf.Name, colname_list, placeholders)
}

func buildUpdateStatement(cf *ColumnFamily, colnames []string) string {
	set_terms := make([]string, len(colnames))
	for i, colname := range colnames {
		set_terms[i] = fmt.Sprintf("%s = ?", colname)
	}
	pkdef := cf.Options.PrimaryKey
	where_terms := make([]string, len(pkdef))
	for i, k := range pkdef {
		where_terms[i] = fmt.Sprintf("%s = ?", k)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		cf.Name, strings.Join(set_terms, ", "), strings.Join(where_terms, " AND "))
}

// LoadByKey loads data from a row's column family into that row. The row is selected by the given
// key values, which must correspond to the column family's defined primary key.
func (orm *Orm) LoadByKey(cf *ColumnFamily, row Persistable, key ...interface{}) error {
	colnames := make([]string, len(cf.Columns))
	for i, col := range cf.Columns {
		colnames[i] = col.Name
	}

	pkdef := cf.Options.PrimaryKey
	rules := make([]string, len(pkdef))
	for i, k := range pkdef {
		rules[i] = fmt.Sprintf("%s = ?", k)
	}

	stmt := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(colnames, ", "), cf.Name, strings.Join(rules, " AND "))
	q := orm.Query(stmt, key...)
	if err := q.Scan(row.loadedColumns().receiverInterfaces(colnames)...); err != nil {
		return err
	}
	return row.loadedColumns().UnmarshalRow(row)
}

// Exists checks if a row exists in a given row's column family.
func (orm *Orm) Exists(cf *ColumnFamily, key ...interface{}) (bool, error) {
	pkdef := cf.Options.PrimaryKey
	rules := make([]string, len(pkdef))
	for i, k := range pkdef {
		rules[i] = fmt.Sprintf("%s = ?", k)
	}

	stmt := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", cf.Name, strings.Join(rules, " AND "))
	q := orm.Query(stmt, key...)
	var count int
	if err := q.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
