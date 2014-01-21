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

// An Orm binds a Schema to a CassandraConn.
type Orm struct {
	*CassandraConn
	Model         *Schema     // The column families to use in this keyspace.
	SchemaUpdates *SchemaDiff // The differences found between the existing column families and the given Model.
	SeqID         SeqIDGenerator
}

// DialOrm establishes a connection to Cassandra and returns an Orm pointer for storing and loading
// Rows according to a given model.
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
func (orm *Orm) Commit(cf *ColumnFamily, row Row, cas bool) error {
	pkdef := cf.Options.PrimaryKey
	mmap := make(MarshalledMap)
	if err := row.Marshal(&mmap); err != nil {
		return err
	}

	// First allow indexes to update both Cassandra and the Row.
	b := gocql.NewBatch(gocql.LoggedBatch)
	for _, idx := range cf.Options.Indexes {
		idx_stmts, err := idx.Index(cf, &mmap)
		if err != nil {
			return err
		}
		for _, idx_stmt := range idx_stmts {
			b.Query(idx_stmt.Query, idx_stmt.Params...)
		}
	}
	if len(b.Entries) > 0 {
		if err := orm.Session.ExecuteBatch(b); err != nil {
			return err
		}
	}

	// Generate the appropriate CQL.
	selectedKeys := make([]string, len(cf.Columns))
	for i, col := range cf.Columns {
		selectedKeys[i] = col.Name
	}
	var stmt string
	var params []interface{}
	if cas {
		stmt = buildInsertStatement(cf, selectedKeys)
		params = mmap.InterfacesFor(selectedKeys...)
	} else {
		// For an update, it makes no sense to treat primary keys as dirty.
		for _, k := range pkdef {
			mmap[k].Dirty = false
		}
		selectedKeys = mmap.DirtyKeys()
		stmt = buildUpdateStatement(cf, selectedKeys)
		params = mmap.InterfacesFor(selectedKeys...)
		for _, v := range mmap.InterfacesFor(pkdef...) {
			params = append(params, v)
		}
	}

	// Apply the computed CQL and check results.
	q := orm.Query(stmt, params...)

	if cas {
		// A CAS query uses ScanCAS for the lightweight transaction. This returns a boolean
		// indicating success, and a row with the values that were committed. We don't need this
		// response, but we need to supply MarshalledValue pointers for the returned columns anyway.
		// Despite this, the values pointed to will not be filled in except in the case of error.
		casmap := make(MarshalledMap)
		pointers := casmap.PointersTo(selectedKeys...)
		if applied, err := q.ScanCAS(pointers...); !applied || err != nil {
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
	// Make the row unmarshal its given values, in case it is caching upon load.
	return row.Unmarshal(&mmap)
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
func (orm *Orm) LoadByKey(cf *ColumnFamily, row Row, key ...interface{}) error {
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
	mmap := make(MarshalledMap)
	if err := q.Scan(mmap.PointersTo(colnames...)...); err != nil {
		return err
	}
	return row.Unmarshal(&mmap)
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
