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

func (orm *Orm) PrepareCommit(cf *ColumnFamily, row Row, cas bool) ([]*CQL, error) {
	mmap := make(MarshalledMap)
	if err := row.Marshal(mmap); err != nil {
		return nil, err
	}
	return orm.precommit(cf, mmap, cas)
}

func (orm *Orm) precommit(cf *ColumnFamily, mmap MarshalledMap, cas bool) ([]*CQL, error) {
	stmts := make([]*CQL, 0, 1)

	// Generate the appropriate CQL.
	selectedKeys := make([]string, len(cf.Columns))
	for i, col := range cf.Columns {
		selectedKeys[i] = col.Name
	}
	var cql *CQL
	if cas {
		cql = NewInsert(cf).Keys(selectedKeys...).Values(mmap.InterfacesFor(selectedKeys...)...)
		cql.IfNotExists()
	} else {
		// For an update, it makes no sense to treat primary keys as dirty.
		for _, k := range cf.Options.PrimaryKey {
			mmap[k].Dirty = false
		}
		selectedKeys = mmap.DirtyKeys()
		if len(selectedKeys) > 0 {
			cql = NewUpdate(cf)
			for _, k := range selectedKeys {
				cql.Set(k, mmap[k])
			}
			for _, k := range cf.Options.PrimaryKey {
				cql.Where(k+" = ?", mmap[k])
			}
		}
	}
	return append(stmts, cql), nil
}

// Commit writes any modified values in the given row to the given CF.
func (orm *Orm) Commit(cf *ColumnFamily, row Row, cas bool) error {
	mmap := make(MarshalledMap)
	if err := row.Marshal(mmap); err != nil {
		return err
	}

	stmts, err := orm.precommit(cf, mmap, cas)
	if err != nil {
		return err
	}

	// Apply all but the final statement as a batch (as these should be index updates).
	if len(stmts) > 1 {
		batch := gocql.NewBatch(gocql.LoggedBatch)
		for _, cql := range stmts[:len(stmts)-1] {
			compiled := cql.compile()
			batch.Query(compiled.term, compiled.params...)
		}
		if err = orm.Session.ExecuteBatch(batch); err != nil {
			return err
		}
	}

	// Apply the INSERT or UPDATE and check results.
	cql := stmts[len(stmts)-1]
	if cql == nil {
		return nil
	}
	compiled := cql.compile()
	q := orm.Query(compiled.term, compiled.params...)
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
	return row.Unmarshal(mmap)
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
	return row.Unmarshal(mmap)
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
