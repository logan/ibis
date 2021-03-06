package ibis

import "errors"
import "fmt"
import "reflect"
import "sort"
import "strings"
import "time"

import "github.com/gocql/gocql"

type fakeKeyspace struct {
	Cluster *fakeCluster
	CFs     map[string]*fakeTable
	Options optionMap
}

func (ks *fakeKeyspace) GetCF(name string) (*fakeTable, error) {
	if ks.CFs == nil {
		ks.CFs = make(map[string]*fakeTable)
	}
	if name == "system.schema_columnfamilies" {
		return ks.Cluster.schemaColumnFamilies(), nil
	}
	if name == "system.schema_columns" {
		return ks.Cluster.schemaColumns(), nil
	}
	cf, ok := ks.CFs[name]
	if !ok {
		return nil, errors.New("column family doesn't exist: " + name)
	}
	return cf, nil
}

func (ks *fakeKeyspace) AddCF(name string, table *fakeTable) {
	if ks.CFs == nil {
		ks.CFs = make(map[string]*fakeTable)
	}
	ks.CFs[name] = table
}

type fakeCluster struct {
	Keyspaces       map[string]*fakeKeyspace
	CurrentKeyspace string
}

// FakeCassandra returns a Cluster interface to an in-memory imitation of Cassandra. This is great
// for unit testing, but beware that the fake implementation is quite rudimentary, incomplete, and
// probably inaccurate.
func FakeCassandra(keyspace string) Cluster {
	c := &fakeCluster{Keyspaces: make(map[string]*fakeKeyspace)}
	c.AddKeyspace("system")
	c.AddKeyspace(keyspace)
	c.CurrentKeyspace = keyspace
	return c
}

func (c *fakeCluster) Close() {}

func (c *fakeCluster) GetKeyspace() string {
	return c.CurrentKeyspace
}

func (c *fakeCluster) AddKeyspace(name string) *fakeKeyspace {
	ks := &fakeKeyspace{Cluster: c}
	c.Keyspaces[name] = ks
	return ks
}

func (c *fakeCluster) Query(stmts ...CQL) Query {
	var results resultSet
	for _, stmt := range stmts {
		parser := newStatement(string(stmt.PreparedCQL))
		if err := parser.Compile(); err != nil {
			return &fakeQuery{err: err}
		}
		ks, ok := c.Keyspaces[c.CurrentKeyspace]
		if !ok || ks == nil {
			return &fakeQuery{err: errors.New("keyspace doesn't exist: " + c.CurrentKeyspace)}
		}
		var err error
		results, err = parser.Execute(c.Keyspaces[c.CurrentKeyspace], stmt.params...)
		if err != nil {
			return &fakeQuery{err: err}
		}
	}
	return &fakeQuery{results: results}
}

func (c *fakeCluster) schemaColumnFamilies() *fakeTable {
	table := &fakeTable{
		Columns: []string{"keyspace_name", "columnfamily_name", "key_aliases", "column_aliases",
			"comment"},
		Key:  []string{"keyspace_name", "columnfamily_name"},
		Rows: make([]MarshaledMap, 0),
	}
	stringList := func(values []string) *MarshaledValue {
		var val string
		if len(values) == 0 {
			val = "[]"
		} else {
			val = fmt.Sprintf(`["%s"]`, strings.Join(values, `", "`))
		}
		return (*MarshaledValue)(LiteralValue(val))
	}
	for ksname, ks := range c.Keyspaces {
		for tname, t := range ks.CFs {
			mmap := make(MarshaledMap)
			mmap["keyspace_name"] = (*MarshaledValue)(LiteralValue(ksname))
			mmap["columnfamily_name"] = (*MarshaledValue)(LiteralValue(tname))
			mmap["key_aliases"] = stringList(t.Key[:1])
			mmap["column_aliases"] = stringList(t.Key[1:])
			mmap["comment"] = (*MarshaledValue)(LiteralValue(""))
			if t.Options != nil {
				if c, ok := t.Options["comment"]; ok {
					mmap["comment"] = (*MarshaledValue)(LiteralValue(c))
				}
			}
			table.Rows = append(table.Rows, mmap)
		}
	}
	return table
}

func (c *fakeCluster) schemaColumns() *fakeTable {
	table := &fakeTable{
		Columns: []string{"keyspace_name", "columnfamily_name", "column_name", "validator"},
		Key:     []string{"keyspace_name", "columnfamily_name"},
		Rows:    make([]MarshaledMap, 0),
	}
	validator := func(coltype *gocql.TypeInfo) string {
		for n, ti := range typeInfoMap {
			if ti == coltype {
				for v, t := range column_validators {
					if t == n {
						return v
					}
				}
			}
		}
		return ""
	}
	for ksname, ks := range c.Keyspaces {
		for tname, t := range ks.CFs {
			for i, colname := range t.Columns {
				mmap := make(MarshaledMap)
				mmap["keyspace_name"] = (*MarshaledValue)(LiteralValue(ksname))
				mmap["columnfamily_name"] = (*MarshaledValue)(LiteralValue(strings.ToLower(tname)))
				mmap["column_name"] = (*MarshaledValue)(LiteralValue(colname))
				mmap["validator"] = (*MarshaledValue)(LiteralValue(validator(t.ColumnTypes[i])))
				table.Rows = append(table.Rows, mmap)
			}
		}
	}
	return table
}

type fakeQuery struct {
	results resultSet
	err     error
}

func (q *fakeQuery) Close() error {
	return q.err
}

func (q *fakeQuery) Exec() error {
	return q.err
}

func (q *fakeQuery) Scan(dests ...interface{}) bool {
	return q.scan(dests, false)
}

func (q *fakeQuery) ScanCAS(dests ...interface{}) bool {
	return q.scan(dests, true)
}

func (q *fakeQuery) scan(dests []interface{}, cas bool) bool {
	if len(q.results) < 1 {
		return false
	}
	result := q.results[0]
	q.results = q.results[1:]
	applied := true
	if cas {
		if result.Columns[0] != "*applied" {
			q.err = errors.New("ScanCAS called on non-CAS query")
			return false
		}
		x, err := unmarshal(result.Row["*applied"])
		if err != nil {
			q.err = err
			return false
		}
		applied = x.(bool)
		result.Columns = result.Columns[1:]
	}
	if len(result.Columns) != len(dests) {
		q.err = errors.New("number of destinations and number of result cols do not match")
		return false
	}
	for i, addr := range dests {
		val := result.Row[result.Columns[i]]
		if val == nil {
			continue
		}
		if err := gocql.Unmarshal(val.TypeInfo, val.Bytes, addr); err != nil {
			q.err = err
			return false
		}
	}
	return applied
}

type optionMap map[string]pval

type fakeTable struct {
	Columns     []string
	ColumnTypes []*gocql.TypeInfo
	Key         []string
	Rows        []MarshaledMap
	Options     optionMap
}

func (t *fakeTable) Get(keyvals []*MarshaledValue) MarshaledMap {
	for _, row := range t.Rows {
		if row.Match(t.Key, keyvals) {
			return row
		}
	}
	return nil
}

func (t *fakeTable) Set(mmap MarshaledMap, cas bool) (MarshaledMap, bool, error) {
	values := mmap.ValuesOf(t.Key...)
	for i, v := range values {
		if v == nil {
			return nil, false, errors.New("key value for " + t.Key[i] + " not given")
		}
	}
	row := t.Get(mmap.ValuesOf(t.Key...))
	if row != nil {
		if cas {
			return row, false, nil
		}
	} else {
		row = make(MarshaledMap)
		t.Rows = append(t.Rows, row)
	}
	for k, v := range mmap {
		row[k] = v
	}
	return row, true, nil
}

type comparison struct {
	col string
	op  string
	val pval
}

func (cmp *comparison) match(row MarshaledMap, binds valueList) (bool, error) {
	left, ok := row[cmp.col]
	if !ok || left == nil {
		return false, nil
	}
	v := (*MarshaledValue)(left)
	c, err := v.cmp(cmp.val.Get(binds))
	if err != nil {
		return false, err
	}
	var result bool
	switch cmp.op {
	case "<=":
		result = c <= 0
	case "<":
		result = c < 0
	case ">=":
		result = c >= 0
	case ">":
		result = c > 0
	case "=":
		result = c == 0
	default:
		return false, errors.New("invalid op " + cmp.op)
	}
	return result, nil
}

type orderDir int

const (
	asc orderDir = iota
	desc
)

type order struct {
	col string
	dir orderDir
}

type sortInterface struct {
	rows  resultSet
	order []order
}

func (s sortInterface) Len() int { return len(s.rows) }

func (s sortInterface) Less(i, j int) bool {
	for _, o := range s.order {
		vi := (*MarshaledValue)(s.rows[i].Row[o.col])
		vj := (*MarshaledValue)(s.rows[j].Row[o.col])
		if vi == nil {
			return o.dir == asc
		}
		if vj == nil {
			return o.dir == desc
		}
		cmp, err := vi.cmp(vj)
		if err != nil {
			return false
		}
		if o.dir == asc {
			return cmp < 0
		} else {
			return cmp > 0
		}
	}
	return false
}

func (s sortInterface) Swap(i, j int) {
	srow := s.rows[i]
	s.rows[i] = s.rows[j]
	s.rows[j] = srow
}

func (rs resultSet) Sort(order []order) {
	if len(order) > 0 {
		sort.Sort(sortInterface{rs, order})
	}
}

func (t *fakeTable) Query(cols []string, where []comparison, binds valueList) (resultSet, error) {
	if len(cols) == 1 {
		if cols[0] == "*" {
			cols = t.Columns
		} else if strings.ToLower(cols[0]) == "count(*)" || strings.ToLower(cols[0]) == "count(1)" {
			cols = nil
		}
	}
	count := 0
	rows := make(resultSet, 0)
	for _, row := range t.Rows {
		rowOk := true
		for _, cmp := range where {
			ok, err := cmp.match(row, binds)
			if err != nil {
				return nil, err
			}
			if !ok {
				rowOk = false
				break
			}
		}
		if rowOk {
			count++
			if cols != nil {
				srow := row.Select(cols)
				rows = append(rows, srow)
			}
		}
	}
	if cols == nil {
		srow := selectedRow{Row: make(MarshaledMap), Columns: []string{"count"}}
		srow.Row["count"] = (*MarshaledValue)(LiteralValue(count))
		rows = append(rows, srow)
	}
	return rows, nil
}

type selectedRow struct {
	Row     MarshaledMap
	Columns []string
}

func (r *selectedRow) String() string {
	parts := make([]string, len(r.Columns))
	for i, col := range r.Columns {
		v, err := unmarshal(r.Row[col])
		if err != nil {
			parts[i] = "<error>"
		} else {
			parts[i] = fmt.Sprintf("%v", v)
		}
	}
	return strings.Join(parts, ",")
}

type resultSet []selectedRow

func (rs resultSet) String() string {
	if len(rs) == 0 {
		return "no results"
	}
	lines := make([]string, len(rs)+1)
	lines[0] = strings.Join(rs[0].Columns, ",")
	for i, row := range rs {
		lines[i+1] = row.String()
	}
	return strings.Join(lines, "\n")
}

func (r *MarshaledMap) Match(key []string, values []*MarshaledValue) bool {
	if len(key) != len(values) {
		return false
	}
	for i, k := range key {
		if values[i] == nil {
			return false
		}
		v, ok := (*r)[k]
		if !ok || v == nil {
			return false
		}
		cmp, err := (*MarshaledValue)(values[i]).cmp((*MarshaledValue)(v))
		if err != nil || cmp != 0 {
			return false
		}
	}
	return true
}

func (r *MarshaledMap) Select(keys []string) selectedRow {
	srow := selectedRow{Row: make(MarshaledMap), Columns: keys}
	for k, v := range *r {
		srow.Row[k] = v
	}
	return srow
}

type valueList []*MarshaledValue

func unmarshal(mval *MarshaledValue) (interface{}, error) {
	if mval == nil {
		return nil, nil
	}

	var addr interface{}
	switch mval.TypeInfo {
	case TIBoolean:
		var b bool
		addr = &b
	case TIBlob:
		var b []byte
		addr = &b
	case TIDouble:
		var f float64
		addr = &f
	case TIBigInt:
		var i int64
		addr = &i
	case TIVarchar:
		var s string
		addr = &s
	case TITimestamp:
		var t time.Time
		addr = &t
	case TIUUID:
		var u gocql.UUID
		addr = &u
	default:
		return nil, errors.New(fmt.Sprintf("don't know how to unmarshal %+v", mval))
	}
	if err := gocql.Unmarshal(mval.TypeInfo, mval.Bytes, addr); err != nil {
		return nil, err
	}
	return reflect.ValueOf(addr).Elem().Interface(), nil
}

func LiteralValue(val interface{}) *MarshaledValue {
	var ti *gocql.TypeInfo
	switch val.(type) {
	case *MarshaledValue:
		return val.(*MarshaledValue)
	case bool:
		ti = TIBoolean
	case []byte:
		ti = TIBlob
	case float64:
		ti = TIDouble
	case int, int64:
		ti = TIBigInt
	case string, SeqID:
		ti = TIVarchar
	case time.Time:
		ti = TITimestamp
	case TimeUUID, gocql.UUID:
		ti = TIUUID
	}
	if ti == nil {
		return nil
	}
	marshaled, err := gocql.Marshal(ti, val)
	if err != nil {
		return nil
	}
	mv := &MarshaledValue{Bytes: marshaled, TypeInfo: ti}
	return (*MarshaledValue)(mv)
}

type pval struct {
	Value    *MarshaledValue
	VarIndex int
}

func (vi pval) Get(binds valueList) *MarshaledValue {
	if vi.Value == nil {
		return binds[vi.VarIndex]
	}
	return vi.Value
}
