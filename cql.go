package ibis

import "fmt"
import "strings"

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

// PreparedCQL is a string containing a CQL statement that may contain placeholders ('?').
type PreparedCQL string

// Bind returns a CQL value, associating a prepared CQL statement with values for its placeholders.
func (pcql PreparedCQL) Bind(params ...interface{}) CQL {
	return CQL{PreparedCQL: pcql, params: params}
}

// CQL is a PreparedCQL value associated with values for its placeholders. CQL values can be passed
// to the Query method of a Cluster. A CQL value can be bound to a cluster with the Cluster method,
// after which it can be executed by calling the Query method.
type CQL struct {
	PreparedCQL
	params  []interface{}
	cluster Cluster
}

// String returns the prepared CQL string.
func (cql CQL) String() string {
	return string(cql.PreparedCQL)
}

// Cluster binds a CQL value to a cluster. If cluster is non-nil, the Query method can then be used.
func (cql *CQL) Cluster(cluster Cluster) {
	cql.cluster = cluster
}

// Query issues a CQL statement on its bound cluster. This requires a valid cluster to have been
// passed to the Cluster method beforehand.
func (cql CQL) Query() Query {
	return cql.cluster.Query(cql)
}

// CQLBuilder is a sequence of CQL values, which may be fragments of proper CQL bound with values
// for placeholders. This is for convenience of constructing CQL programmatically in a declarative
// fashion.
type CQLBuilder []CQL

func (b CQLBuilder) join(prefix, conn string) (result CQL) {
	if len(b) == 0 {
		return
	}
	terms := make([]string, len(b))
	np := 0
	for i, p := range b {
		terms[i] = string(p.PreparedCQL)
		if p.params != nil {
			np += len(p.params)
		}
	}
	result.PreparedCQL = PreparedCQL(prefix + strings.Join(terms, conn))
	result.params = make([]interface{}, 0, np)
	for _, p := range b {
		if p.params != nil {
			result.params = append(result.params, p.params...)
		}
	}
	return
}

// CQL combines all of the fragments of the builder into a single CQL value.
func (b CQLBuilder) CQL() CQL {
	return b.join("", "")
}

// Clear reinitializes the builder to an empty state.
func (b *CQLBuilder) Clear() *CQLBuilder {
	*b = make(CQLBuilder, 0)
	return b
}

// Append adds a CQL fragment to the end. Values for placeholders in the fragment may be given as
// additional arguments.
func (b *CQLBuilder) Append(term string, params ...interface{}) *CQLBuilder {
	if *b == nil {
		*b = make(CQLBuilder, 0)
	}
	*b = append(*b, CQL{PreparedCQL: PreparedCQL(term), params: params})
	return b
}

// AppendCQL adds a CQL value as a fragment to the end of the builder. If the given CQL has bound
// values for placeholders, they are included.
func (b *CQLBuilder) AppendCQL(cql CQL) *CQLBuilder {
	return b.Append(string(cql.PreparedCQL), cql.params...)
}

// SelectBuilder provides a declarative interface for building CQL SELECT statements.
type SelectBuilder struct {
	cf      *ColumnFamily
	cols    []string
	where   CQLBuilder
	orderBy CQLBuilder
	limit   int
}

// Select initializes and returns a SelectBuilder. If no arguments are given, it is initialized as
// a "SELECT * FROM..." on some table. Otherwise, the given arguments will be used to declare the
// columns to select. (They may also be given later by the Cols method).
//
//   Select().From(model.UsersTable).Where("Name = ?", "logan")
//   Select("Name", "Email").From(model.UsersTable).Limit(100)
func Select(keys ...string) *SelectBuilder {
	sel := &SelectBuilder{cols: keys}
	if len(keys) == 0 || (len(keys) == 1 && keys[0] == "*") {
		sel.cols = nil
	}
	return sel
}

// From gives the column family to select from.
func (sel *SelectBuilder) From(cf *ColumnFamily) *SelectBuilder {
	sel.cf = cf
	return sel
}

// Where specifies a term for the WHERE clause of the statement. If Where is called multiple times
// on a builder, the given terms will be combined with the AND operator.
func (sel *SelectBuilder) Where(term string, params ...interface{}) *SelectBuilder {
	sel.where.Append(term, params...)
	return sel
}

// OrderBy specifies an ordering. Each additional call to OrderBy specifies the next tiebreaker
// for sorting returned rows.
func (sel *SelectBuilder) OrderBy(term string) *SelectBuilder {
	sel.orderBy.Append(term)
	return sel
}

// Limit specifies a limit on the number of returned rows.
func (sel *SelectBuilder) Limit(limit int) *SelectBuilder {
	sel.limit = limit
	return sel
}

// CQL compiles the built select statement.
func (sel *SelectBuilder) CQL() CQL {
	var b CQLBuilder
	b.Append("SELECT ")
	if sel.cols == nil || (len(sel.cols) == 1 && sel.cols[0] == "*") {
		sel.cols = make([]string, len(sel.cf.Columns))
		for i, col := range sel.cf.Columns {
			sel.cols[i] = col.Name
		}
	}
	b.Append(strings.Join(sel.cols, ", "))
	b.Append(" FROM ")
	b.Append(sel.cf.Name)
	if sel.where != nil {
		b.AppendCQL(sel.where.join(" WHERE ", " AND "))
	}
	if sel.orderBy != nil {
		b.AppendCQL(sel.orderBy.join(" ORDER BY ", ", "))
	}
	if sel.limit != 0 {
		b.Append(fmt.Sprintf(" LIMIT %d", sel.limit))
	}
	cql := b.CQL()
	cql.Cluster(sel.cf.Cluster())
	return cql
}

func (sel *SelectBuilder) Query() Query {
	return sel.CQL().Query()
}

// InsertBuilder provides a declarative interface for building CQL INSERT statements.
type InsertBuilder struct {
	cf     *ColumnFamily
	keys   []string
	values []interface{}
	cas    bool
}

// InsertInto initializes and returns an InsertBuilder for declaring an insert statement on the
// given column family.
//
//   InsertInto(model.Pets).Keys("Name", "Color").Values("Ezzie", "black").IfNotExists()
func InsertInto(cf *ColumnFamily) *InsertBuilder {
	return &InsertBuilder{cf: cf, keys: make([]string, 0), values: make([]interface{}, 0)}
}

// Keys specifies the names of columns to insert.
func (ins *InsertBuilder) Keys(keys ...string) *InsertBuilder {
	ins.keys = append(ins.keys, keys...)
	return ins
}

// Values specifies the values of the inserted columns.
func (ins *InsertBuilder) Values(values ...interface{}) *InsertBuilder {
	ins.values = append(ins.values, values...)
	return ins
}

// IfNotExists turns this into a CAS statement by appending IF NOT EXISTS.
func (ins *InsertBuilder) IfNotExists() *InsertBuilder {
	ins.cas = true
	return ins
}

// CQL compiles the built insert statement.
func (ins *InsertBuilder) CQL() CQL {
	var b CQLBuilder
	b.Append("INSERT INTO ")
	b.Append(ins.cf.Name)
	b.Append(" (")
	b.Append(strings.Join(ins.keys, ", "))
	b.Append(") VALUES (")
	b.Append(placeholderList(len(ins.values)), ins.values...)
	b.Append(")")
	if ins.cas {
		b.Append(" IF NOT EXISTS")
	}
	cql := b.CQL()
	cql.Cluster(ins.cf.Cluster())
	return cql
}

func (ins *InsertBuilder) Query() Query {
	return ins.CQL().Query()
}

// UpdateBuilder provides a declarative interface for building CQL UPDATE statements.
type UpdateBuilder struct {
	cf    *ColumnFamily
	set   CQLBuilder
	where CQLBuilder
}

// Update initializes and returns an UpdateBuilder for declaring an update statement on the given
// column family.
//
//   Update(model.Users).Set("Password", "hunter2").Where("Name = ?", "logan")
func Update(cf *ColumnFamily) *UpdateBuilder {
	return &UpdateBuilder{cf: cf, set: make(CQLBuilder, 0), where: make(CQLBuilder, 0)}
}

// Set provides a key and value to assign. Call this method for each key-value pair to update.
func (upd *UpdateBuilder) Set(key string, value interface{}) *UpdateBuilder {
	upd.set.Append(key+" = ?", value)
	return upd
}

// Where specifies a term for the WHERE clause of the statement. If Where is called multiple times
// on a builder, the given terms will be combined with the AND operator.
func (upd *UpdateBuilder) Where(term string, params ...interface{}) *UpdateBuilder {
	upd.where.Append(term, params...)
	return upd
}

// CQL compiles the built update statement.
func (upd *UpdateBuilder) CQL() CQL {
	var b CQLBuilder
	b.Append("UPDATE " + upd.cf.Name)
	b.AppendCQL(upd.set.join(" SET ", ", "))
	b.AppendCQL(upd.where.join(" WHERE ", " AND "))
	cql := b.CQL()
	cql.Cluster(upd.cf.Cluster())
	return cql
}

func (upd *UpdateBuilder) Query() Query {
	return upd.CQL().Query()
}

// DeleteBuilder provides a declarative interface for building CQL DELETE statements.
type DeleteBuilder struct {
	cf    *ColumnFamily
	where CQLBuilder
}

// DeleteFrom initializes and returns a DeleteBuilder for declaring a delete statement on the given
// column family.
//
//   DeleteFrom(model.Users).Where("Name = ?", "logan")
func DeleteFrom(cf *ColumnFamily) *DeleteBuilder {
	return &DeleteBuilder{cf: cf, where: make(CQLBuilder, 0)}
}

// Where specifies a term for the WHERE clause of the statement. If Where is called multiple times
// on a builder, the given terms will be combined with the AND operator.
func (del *DeleteBuilder) Where(term string, params ...interface{}) *DeleteBuilder {
	del.where.Append(term, params...)
	return del
}

// CQL compiles the built delete statement.
func (del *DeleteBuilder) CQL() CQL {
	cql := del.where.join("DELETE FROM "+del.cf.Name+" WHERE ", " AND ")
	cql.Cluster(del.cf.Cluster())
	return cql
}

func (del *DeleteBuilder) Query() Query {
	return del.CQL().Query()
}
