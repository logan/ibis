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

type PreparedCQL string

func (pcql PreparedCQL) Bind(params ...interface{}) CQL {
	return CQL{PreparedCQL: pcql, params: params}
}

type CQL struct {
	PreparedCQL
	params  []interface{}
	cluster Cluster
}

func (cql CQL) String() string {
	return string(cql.PreparedCQL)
}

func (cql *CQL) Cluster(cluster Cluster) {
	cql.cluster = cluster
}

func (cql CQL) Query() Query {
	return cql.cluster.Query(cql)
}

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

func (b CQLBuilder) CQL() CQL {
	return b.join("", "")
}

func (b *CQLBuilder) Clear() *CQLBuilder {
	*b = make(CQLBuilder, 0)
	return b
}

func (b *CQLBuilder) Append(term string, params ...interface{}) *CQLBuilder {
	if *b == nil {
		*b = make(CQLBuilder, 0)
	}
	*b = append(*b, CQL{PreparedCQL: PreparedCQL(term), params: params})
	return b
}

func (b *CQLBuilder) AppendCQL(cql CQL) *CQLBuilder {
	return b.Append(string(cql.PreparedCQL), cql.params...)
}

type SelectBuilder struct {
	cf      *ColumnFamily
	cols    []string
	where   CQLBuilder
	orderBy CQLBuilder
	limit   int
}

func Select(keys ...string) *SelectBuilder {
	sel := &SelectBuilder{cols: keys}
	if len(keys) == 0 || (len(keys) == 1 && keys[0] == "*") {
		sel.cols = nil
	}
	return sel
}

func (sel *SelectBuilder) From(cf *ColumnFamily) *SelectBuilder {
	sel.cf = cf
	return sel
}

func (sel *SelectBuilder) Where(term string, params ...interface{}) *SelectBuilder {
	sel.where.Append(term, params...)
	return sel
}

func (sel *SelectBuilder) OrderBy(term string) *SelectBuilder {
	sel.orderBy.Append(term)
	return sel
}

func (sel *SelectBuilder) Limit(limit int) *SelectBuilder {
	sel.limit = limit
	return sel
}

func (sel *SelectBuilder) CQL() CQL {
	var b CQLBuilder
	b.Append("SELECT ")
	cols := sel.cols
	if cols == nil || (len(cols) == 1 && cols[0] == "*") {
		cols = make([]string, len(sel.cf.Columns))
		for i, col := range sel.cf.Columns {
			cols[i] = col.Name
		}
	}
	b.Append(strings.Join(cols, ", "))
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

type InsertBuilder struct {
	cf     *ColumnFamily
	keys   []string
	values []interface{}
	cas    bool
}

func InsertInto(cf *ColumnFamily) *InsertBuilder {
	return &InsertBuilder{cf: cf, keys: make([]string, 0), values: make([]interface{}, 0)}
}

func (ins *InsertBuilder) Keys(keys ...string) *InsertBuilder {
	ins.keys = append(ins.keys, keys...)
	return ins
}

func (ins *InsertBuilder) Values(values ...interface{}) *InsertBuilder {
	ins.values = append(ins.values, values)
	return ins
}

func (ins *InsertBuilder) IfNotExists() *InsertBuilder {
	ins.cas = true
	return ins
}

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

type UpdateBuilder struct {
	cf    *ColumnFamily
	set   CQLBuilder
	where CQLBuilder
}

func Update(cf *ColumnFamily) *UpdateBuilder {
	return &UpdateBuilder{cf: cf, set: make(CQLBuilder, 0), where: make(CQLBuilder, 0)}
}

func (upd *UpdateBuilder) Set(key string, value interface{}) *UpdateBuilder {
	upd.set.Append(key+" = ?", value)
	return upd
}

func (upd *UpdateBuilder) Where(term string, params ...interface{}) *UpdateBuilder {
	upd.where.Append(term, params...)
	return upd
}

func (upd *UpdateBuilder) CQL() CQL {
	var b CQLBuilder
	b.Append("UPDATE " + upd.cf.Name)
	b.AppendCQL(upd.set.join(" SET ", ", "))
	b.AppendCQL(upd.where.join(" WHERE ", " AND "))
	cql := b.CQL()
	cql.Cluster(upd.cf.Cluster())
	return cql
}

type DeleteBuilder struct {
	cf    *ColumnFamily
	where CQLBuilder
}

func DeleteFrom(cf *ColumnFamily) *DeleteBuilder {
	return &DeleteBuilder{cf: cf, where: make(CQLBuilder, 0)}
}

func (del *DeleteBuilder) Where(term string, params ...interface{}) *DeleteBuilder {
	del.where.Append(term, params...)
	return del
}

func (del *DeleteBuilder) CQL() CQL {
	cql := del.where.join("DELETE FROM "+del.cf.Name+" WHERE ", " AND ")
	cql.Cluster(del.cf.Cluster())
	return cql
}
