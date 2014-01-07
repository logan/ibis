package datastore

import "errors"
import "fmt"
import "reflect"
import "strings"

type TableCache map[reflect.Type]*Table

var tableCache TableCache = make(TableCache)

type StorableData map[string]interface{}

type Storable interface {
	loadedColumns() StorableData
}

type Stored struct {
	_loadedColumns StorableData
}

func (s *Stored) loadedColumns() StorableData {
	if s._loadedColumns == nil {
		s._loadedColumns = make(StorableData)
	}
	return s._loadedColumns
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

func (c *CassandraConn) Create(row Storable) error {
	return c.commit(row, true)
}

func (c *CassandraConn) Commit(row Storable) error {
	return c.commit(row, false)
}

func (c *CassandraConn) commit(row Storable, ifnotexists bool) error {
	row_type := reflect.TypeOf(row)
	table, ok := tableCache[row_type]
	if !ok {
		return errors.New(fmt.Sprintf("no table registered for type %v", row_type))
	}
	newk, newv := getColumnsToCommit(table, row)
	if len(newk) == 0 {
		return nil
	}
	var ifne string
	if ifnotexists {
		ifne = " IF NOT EXISTS"
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)%s",
		table.Name, strings.Join(newk, ", "), placeholderList(len(newk)), ifne)
	q := c.Session.Query(stmt, newv...)
	var applied bool
	if err := q.Scan(&applied); err != nil {
		return err
	}
	if !applied {
		return errors.New("insert failed")
	}
	loadedColumns := row.loadedColumns()
	for i, col := range newk {
		loadedColumns[col] = newv[i]
	}
	return nil
}

func getColumnsToCommit(table *Table, row Storable) (newk []string, newv []interface{}) {
	values := getColumnValues(table, row)
	newk = make([]string, 0, len(values))
	newv = make([]interface{}, 0, len(values))
	loadedColumns := row.loadedColumns()
	for col, val := range values {
		orig, ok := loadedColumns[col]
		if !ok || orig != val {
			newk = append(newk, col)
			newv = append(newv, val)
		}
	}
	return
}

func getColumnValues(table *Table, row Storable) map[string]interface{} {
	value := reflect.Indirect(reflect.ValueOf(row))
	result := make(StorableData)
	for _, col := range table.Columns {
		fieldv := value.FieldByName(col.Name)
		if fieldv.IsValid() {
			result[col.Name] = fieldv.Interface()
		}
	}
	return result
}

func (c *CassandraConn) LoadByKey(row Storable, key ...interface{}) error {
	ptr_type := reflect.TypeOf(row)
	ptr_value := reflect.ValueOf(row)
	row_value := reflect.Indirect(ptr_value)
	table := tableCache[ptr_type]
	pkdef := table.Options.PrimaryKey
	cols := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		cols[i] = col.Name
	}
	rules := make([]string, len(pkdef))
	for i, k := range pkdef {
		rules[i] = fmt.Sprintf("%s = ?", k)
	}
	fields := make([]reflect.StructField, len(table.Columns))
	dests := make([]interface{}, len(table.Columns))
	for i, col := range table.Columns {
		fields[i], _ = row_value.Type().FieldByName(col.Name)
		dests[i] = reflect.New(fields[i].Type).Interface()
	}
	stmt := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(cols, ", "), table.Name, strings.Join(rules, " AND "))
	q := c.Session.Query(stmt, key...)
	if err := q.Scan(dests...); err != nil {
		return err
	}
	loadedColumns := row.loadedColumns()
	for i, v := range dests {
		loadedColumns[cols[i]] = v
		row_value.FieldByIndex(fields[i].Index).Set(reflect.Indirect(reflect.ValueOf(v)))
	}
	return nil
}
