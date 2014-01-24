package datastore

import "fmt"
import "strings"

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
	raw     boundPart
}

func (cql *CQL) Build() (string, []interface{}) {
	bp := cql.compile()
	return bp.term, bp.params
}

func (cql *CQL) compile() boundPart {
	switch cql.cmd {
	case "":
		return cql.raw
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

func (cql *CQL) Query() Query {
	return cql.cf.orm.Cluster.Query(cql)
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

func NewCQL(stmt string, params ...interface{}) *CQL {
	return &CQL{cmd: "", raw: boundPart{stmt, params}}
}

func NewSelect(cf *ColumnFamily) *CQL {
	return &CQL{cf: cf, cmd: "SELECT"}
}

func NewInsert(cf *ColumnFamily) *CQL {
	return &CQL{cf: cf, cmd: "INSERT"}
}

func NewUpdate(cf *ColumnFamily) *CQL {
	return &CQL{cf: cf, cmd: "UPDATE"}
}

func NewDelete(cf *ColumnFamily) *CQL {
	return &CQL{cf: cf, cmd: "DELETE"}
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
