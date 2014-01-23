package datastore

import "errors"
import "strings"

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrTableNotBound = errors.New("table not bound; call table.Bind(orm)")
	ErrInvalidType   = errors.New("invalid row type")
)

// An Orm binds a Schema to a Cluster.
type Orm struct {
	Cluster
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
	orm := &Orm{Cluster: conn, Model: model}
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
		for _, k := range cf.PrimaryKey {
			mmap[k].Dirty = false
		}
		selectedKeys = mmap.DirtyKeys()
		if len(selectedKeys) > 0 {
			cql = NewUpdate(cf)
			for _, k := range selectedKeys {
				cql.Set(k, mmap[k])
			}
			for _, k := range cf.PrimaryKey {
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
		qiter := orm.Cluster.Query(stmts[:len(stmts)-1]...)
		if err = qiter.Close(); err != nil {
			return err
		}
	}

	// Apply the INSERT or UPDATE and check results.
	cql := stmts[len(stmts)-1]
	if cql == nil {
		return nil
	}
	qiter := orm.Cluster.Query(cql)
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

// LoadByKey loads data from a row's column family into that row. The row is selected by the given
// key values, which must correspond to the column family's defined primary key.
func (orm *Orm) LoadByKey(cf *ColumnFamily, row Row, key ...interface{}) error {
	colnames := make([]string, len(cf.Columns))
	for i, col := range cf.Columns {
		colnames[i] = col.Name
	}

	cql := NewSelect(cf).Cols(colnames...)
	for i, k := range cf.PrimaryKey {
		cql.Where(k+" = ?", key[i])
	}
	qiter := orm.Query(cql)
	mmap := make(MarshalledMap)
	if !qiter.Scan(mmap.PointersTo(colnames...)...) {
		return qiter.Close()
	}
	return row.Unmarshal(mmap)
}

// Exists checks if a row exists in a given row's column family.
func (orm *Orm) Exists(cf *ColumnFamily, key ...interface{}) (bool, error) {
	cql := NewSelect(cf).Cols("COUNT(*)")
	for i, k := range cf.PrimaryKey {
		cql.Where(k+" = ?", key[i])
	}
	qiter := orm.Query(cql)
	var count int
	if !qiter.Scan(&count) {
		return false, qiter.Close()
	}
	return count > 0, nil
}
