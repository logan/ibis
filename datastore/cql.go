package datastore

import "fmt"
import "strings"

import "tux21b.org/v1/gocql"

type CQL struct {
	cf      *ColumnFamily
	cmd     string
	cols    []string
	where   boundPartGroup
	orderBy boundPartGroup
	keys    []string
	vals    []interface{}
	set     boundPartGroup
	limit   int
	cas     bool
}

func (cql *CQL) compile() boundPart {
	switch cql.cmd {
	case "SELECT":
		where := cql.where.join("WHERE", " AND ")
		orderBy := cql.orderBy.join("ORDER BY", ", ")
		if cql.cols == nil || len(cql.cols) == 0 {
			cql.cols = make([]string, 0, len(cql.cf.Columns))
			for _, col := range cql.cf.Columns {
				cql.cols = append(cql.cols, col.Name)
			}
		}
		var limit boundPart
		if cql.limit > 0 {
			limit.term = fmt.Sprintf("LIMIT %d", cql.limit)
		}
		prefix := boundPart{
			fmt.Sprintf("%s FROM %s", strings.Join(cql.cols, ", "), cql.cf.Name),
			nil,
		}
		return boundPartGroup{prefix, where, orderBy, limit}.join(cql.cmd, " ")
	case "INSERT":
		var ifne string
		if cql.cas {
			ifne = " IF NOT EXISTS"
		}
		return boundPart{
			fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)%s", cql.cf.Name,
				strings.Join(cql.keys, ", "), placeholderList(len(cql.keys)), ifne),
			cql.vals,
		}
	case "UPDATE":
		prefix := boundPart{cql.cf.Name, nil}
		set := cql.set.join("SET", ", ")
		where := cql.where.join("WHERE", " AND ")
		return boundPartGroup{prefix, set, where}.join(cql.cmd, " ")
	case "DELETE":
		prefix := boundPart{"FROM " + cql.cf.Name, nil}
		where := cql.where.join("WHERE", " AND ")
		return boundPartGroup{prefix, where}.join(cql.cmd, " ")
	}
	return boundPart{}
}

func (cql *CQL) String() string {
	compiled := cql.compile()
	return compiled.term
}

func (cql *CQL) Query() *CQLQuery {
	compiled := cql.compile()
	q := &CQLQuery{
		cql: cql,
		q:   cql.cf.orm.Query(compiled.term, compiled.params...),
	}
	q.i = q.Iter()
	return q
}

func (cql *CQL) Cols(keys ...string) *CQL {
	if len(keys) == 1 && keys[0] == "*" {
		cql.cols = nil
	} else {
		if cql.cols == nil {
			cql.cols = make([]string, 0, len(keys))
		}
		cql.cols = append(cql.cols, keys...)
	}
	return cql
}

func (cql *CQL) Where(term string, params ...interface{}) *CQL {
	if cql.where == nil {
		cql.where = make(boundPartGroup, 0, 2)
	}
	cql.where = append(cql.where, boundPart{term, params})
	return cql
}

func (cql *CQL) OrderBy(term string) *CQL {
	if cql.orderBy == nil {
		cql.orderBy = make(boundPartGroup, 0, 1)
	}
	cql.orderBy = append(cql.orderBy, boundPart{term, nil})
	return cql
}

func (cql *CQL) Keys(keys ...string) *CQL {
	cql.keys = keys
	return cql
}

func (cql *CQL) Values(values ...interface{}) *CQL {
	cql.vals = values
	return cql
}

func (cql *CQL) IfNotExists() *CQL {
	cql.cas = true
	return cql
}

func (cql *CQL) Set(key string, value interface{}) *CQL {
	if cql.set == nil {
		cql.set = make(boundPartGroup, 0, len(cql.cf.Columns))
	}
	cql.set = append(cql.set, boundPart{key + " = ?", []interface{}{value}})
	return cql
}

func (cql *CQL) Limit(limit int) *CQL {
	cql.limit = limit
	return cql
}

func NewCQL(cf *ColumnFamily, cmd string) *CQL {
	return &CQL{
		cf:  cf,
		cmd: cmd,
	}
}

func NewSelect(cf *ColumnFamily) *CQL {
	return NewCQL(cf, "SELECT")
}

func NewInsert(cf *ColumnFamily) *CQL {
	return NewCQL(cf, "INSERT")
}

func NewUpdate(cf *ColumnFamily) *CQL {
	return NewCQL(cf, "UPDATE")
}

func NewDelete(cf *ColumnFamily) *CQL {
	return NewCQL(cf, "DELETE")
}

type boundPart struct {
	term   string
	params []interface{}
}

type boundPartGroup []boundPart

func (parts boundPartGroup) join(prefix, conn string) (result boundPart) {
	if len(parts) == 0 {
		return
	}
	terms := make([]string, len(parts))
	np := 0
	for i, p := range parts {
		terms[i] = p.term
		if p.params != nil {
			np += len(p.params)
		}
	}
	result.term = strings.TrimSpace(prefix + " " + strings.Join(terms, conn))
	result.params = make([]interface{}, 0, np)
	for _, p := range parts {
		if p.params != nil {
			result.params = append(result.params, p.params...)
		}
	}
	return
}

type CQLQuery struct {
	cql *CQL
	q   *gocql.Query
	i   *CQLIter
}

func (q *CQLQuery) Iter() *CQLIter {
	return &CQLIter{q.cql, q.q.Iter()}
}

func (q *CQLQuery) Scan(mmap *MarshalledMap) error {
	q.i.Next(mmap)
	return q.i.Close()
}

type CQLIter struct {
	cql *CQL
	i   *gocql.Iter
}

func (i *CQLIter) Next(mmap *MarshalledMap) bool {
	return i.i.Scan(mmap.PointersTo(i.cql.cols...)...)
}

func (i *CQLIter) Close() error {
	return i.i.Close()
}
