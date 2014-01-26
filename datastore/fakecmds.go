package datastore

import "errors"

import "tux21b.org/v1/gocql"

type command interface {
	Execute(*fakeKeyspace, valueList) (resultSet, error)
}

type createKeyspaceCommand struct {
	identifier string
	strict     bool
	options    optionMap
}

func (cmd *createKeyspaceCommand) Execute(ks *fakeKeyspace, vals valueList) (resultSet, error) {
	if _, ok := ks.Cluster.Keyspaces[cmd.identifier]; ok {
		if cmd.strict {
			return nil, errors.New("keyspace already exists: " + cmd.identifier)
		}
	}
	ks = ks.Cluster.AddKeyspace(cmd.identifier)
	ks.Cluster.CurrentKeyspace = cmd.identifier
	ks.Options = cmd.options
	return resultSet{}, nil
}

type createTableCommand struct {
	strict     bool
	identifier string
	colnames   []string
	coltypes   []*gocql.TypeInfo
	key        []string
	options    optionMap
}

func (cmd *createTableCommand) Execute(ks *fakeKeyspace, vals valueList) (resultSet, error) {
	if _, ok := ks.CFs[cmd.identifier]; ok && cmd.strict {
		return nil, errors.New("table " + cmd.identifier + " already exists")
	}
	table := &fakeTable{
		Columns:     cmd.colnames,
		ColumnTypes: cmd.coltypes,
		Key:         cmd.key,
		Options:     cmd.options,
		Rows:        make([]MarshalledMap, 0),
	}
	ks.CFs[cmd.identifier] = table
	return resultSet{}, nil
}

type dropCommand struct {
	dropType   string
	identifier string
	strict     bool
}

func (cmd *dropCommand) Execute(ks *fakeKeyspace, vals valueList) (resultSet, error) {
	switch cmd.dropType {
	case "keyspace":
		if _, ok := ks.Cluster.Keyspaces[cmd.identifier]; !ok {
			if cmd.strict {
				return nil, errors.New("keyspace doesn't exist: " + cmd.identifier)
			}
		} else {
			delete(ks.Cluster.Keyspaces, cmd.identifier)
		}
		return resultSet{}, nil
	default:
		return nil, errors.New("drop of " + cmd.dropType + " not implemented")
	}
}

type insertCommand struct {
	table  string
	keys   []string
	values []pval
	cas    bool
}

func (cmd *insertCommand) Execute(ks *fakeKeyspace, vals valueList) (resultSet, error) {
	cf, err := ks.GetCF(cmd.table)
	if err != nil {
		return nil, err
	}
	if len(cmd.keys) != len(cmd.values) {
		return nil, errors.New("number of keys and number of values do not match")
	}
	mmap := make(MarshalledMap)
	for i, k := range cmd.keys {
		mmap[k] = (*MarshalledValue)(cmd.values[i].Get(vals))
	}
	row, applied, err := cf.Set(mmap, cmd.cas)
	if err != nil {
		return nil, err
	}
	srow := row.Select(cmd.keys)
	if cmd.cas {
		srow.Columns = append([]string{"*applied"}, srow.Columns...)
		srow.Row["*applied"] = (*MarshalledValue)(LiteralValue(applied))
	}
	return resultSet{srow}, nil
}

type selectCommand struct {
	table string
	cols  []string
	where []comparison
	order []order
	limit int
}

func (cmd *selectCommand) Execute(ks *fakeKeyspace, vals valueList) (resultSet, error) {
	cf, err := ks.GetCF(cmd.table)
	if err != nil {
		return nil, err
	}
	rows, err := cf.Query(cmd.cols, cmd.where, vals)
	if err != nil {
		return nil, err
	}
	rows.Sort(cmd.order)
	if cmd.limit > 0 && cmd.limit < len(rows) {
		return rows[:cmd.limit], nil
	}
	return rows, nil
}

type updateCommand struct {
	table string
	set   map[string]pval
	key   map[string]pval
}

func (cmd *updateCommand) Execute(ks *fakeKeyspace, vals valueList) (resultSet, error) {
	cf, err := ks.GetCF(cmd.table)
	if err != nil {
		return nil, err
	}
	mmap := make(MarshalledMap)
	for k, v := range cmd.set {
		mmap[k] = (*MarshalledValue)(v.Get(vals))
	}
	for k, v := range cmd.key {
		mmap[k] = (*MarshalledValue)(v.Get(vals))
	}
	if _, _, err := cf.Set(mmap, false); err != nil {
		return nil, err
	}
	return resultSet{}, nil
}

type alterCommand struct {
	table   string
	add     string
	alter   string
	drop    string
	coltype *gocql.TypeInfo
	options optionMap
}

func (cmd *alterCommand) Execute(ks *fakeKeyspace, vals valueList) (resultSet, error) {
	cf, err := ks.GetCF(cmd.table)
	if err != nil {
		return nil, err
	}
	if cmd.options != nil {
		cf.Options = cmd.options
		return resultSet{}, nil
	}
	found := -1
	for i, col := range cf.Columns {
		if cmd.add == col {
			return nil, errors.New("column " + col + " already exists")
		}
		if cmd.alter == col {
			cf.ColumnTypes[i] = cmd.coltype
			found = i
		}
		if cmd.drop == col {
			found = i
		}
	}
	if cmd.add != "" {
		cf.Columns = append(cf.Columns, cmd.add)
		cf.ColumnTypes = append(cf.ColumnTypes, cmd.coltype)
		return resultSet{}, nil
	}
	if found == -1 {
		return nil, errors.New("no such cmdumn: " + cmd.alter + cmd.drop)
	}
	if cmd.drop != "" {
		cf.Columns = append(cf.Columns[:found], cf.Columns[found+1:]...)
		cf.ColumnTypes = append(cf.ColumnTypes[:found], cf.ColumnTypes[found+1:]...)
	}
	return resultSet{}, nil
}
