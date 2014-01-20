package datastore

type OnCreateHook func(*Orm, *ColumnFamily) error

type CFIndexStatement struct {
	Query  string
	Params []interface{}
}

type CFIndex interface {
	// TODO: remove cf arg?
	Index(cf *ColumnFamily, rowValues RowValues) ([]CFIndexStatement, error)
	CFs() []*ColumnFamily
}

// CFOptions is used to provide additional properties for a column family definition.
type CFOptions struct {
	CF            *ColumnFamily
	PrimaryKey    []string
	Indexes       []CFIndex
	onCreateHooks []OnCreateHook // If given, will be called immediately after table creation.
	typeID        int
	ctx           map[interface{}]interface{}
}

func NewCFOptions(cf *ColumnFamily) *CFOptions {
	o := &CFOptions{CF: cf}
	o.Indexes = make([]CFIndex, 0)
	o.ctx = make(map[interface{}]interface{})
	o.onCreateHooks = make([]OnCreateHook, 0)
	return o
}

func (o *CFOptions) Get(key interface{}) interface{} {
	return o.ctx[key]
}

func (o *CFOptions) Set(key, value interface{}) {
	o.ctx[key] = value
}

func (o *CFOptions) Key(keys ...string) *CFOptions {
	o.PrimaryKey = keys

	// primary key columns must come first and in order
	rearranged := make([]Column, len(o.CF.Columns))
	keymap := make(map[string]bool)
	for i, k := range keys {
		for _, col := range o.CF.Columns {
			if k == col.Name {
				keymap[k] = true
				rearranged[i] = col
				break
			}
		}
	}
	i := len(keys)
	for _, col := range o.CF.Columns {
		if _, ok := keymap[col.Name]; !ok {
			rearranged[i] = col
			i++
		}
	}
	copy(o.CF.Columns, rearranged)

	return o
}

func (o *CFOptions) Index(index CFIndex) *CFOptions {
	o.Indexes = append(o.Indexes, index)
	return o
}

func (o *CFOptions) OnCreate(hook OnCreateHook) *CFOptions {
	o.onCreateHooks = append(o.onCreateHooks, hook)
	return o
}
